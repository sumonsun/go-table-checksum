package main

import (
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"flag"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cheggaaa/pb"
	"github.com/ngaut/log"

	_ "github.com/go-sql-driver/mysql"
)

type struct_result struct {
	query_dbs_result *sql.Rows
	query_dba_result *sql.Rows
}
type struct_result_md5 struct {
	query_dbs_md5 string
	query_dba_md5 string
}

func main() {
	ch_result := make(chan struct_result, 1000)
	ch_md5 := make(chan struct_result_md5, 1000)
	t1 := time.Now()
	//初始化命令行参数
	shost, sport, ahost, aport, user, passowrd, database, table, chunksize := help()
	dbs, dba := db_conn(shost, sport, ahost, aport, user, passowrd, database)
	sql_setrr := "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ" //设置session事务隔离级别为RR
	dbs.Query(sql_setrr)
	dba.Query(sql_setrr)
	var sql_exec, sql_pri string

	//查询主键列并做比较，不同则退出程序
	sql_pri = "show index from " + database + "." + table
	query_dbs_pri, err := dbs.Query(sql_pri)
	checkErr(err)
	query_dba_pri, err := dba.Query(sql_pri)
	checkErr(err)
	pri_key_dbs := str_pri(printResult(query_dbs_pri))
	pri_key_dba := str_pri(printResult(query_dba_pri))

	var pri_key string
	pri_key = pri_key_dba

	//查询总行数大小，不相同直接中断
	sql_count := "select  TABLE_ROWS from information_schema.tables where table_schema=" + "'" + database + "'" + " AND " + "table_name=" + "'" + table + "'"
	query_dbs_coount, err := dbs.Query(sql_count)
	checkErr(err)
	query_dba_coount, err := dba.Query(sql_count)
	count_dbs := printResult(query_dbs_coount)
	count_dba := printResult(query_dba_coount)
	checkErr(err)
	if count_dbs == count_dba {
		log.Debugf("总行数相同,行数为:%s", count_dbs)
	} else {
		log.Debugf("总行数不同")
		panic("")
	}

	//判断表的字段名和字段类型，不相同直接断开
	sql_exec = "select * from " + table + " order by " + pri_key + " limit 1"
	query1, err := dbs.Query(sql_exec)
	checkErr(err)
	var row1 sql.Rows = *query1

	query2, err := dba.Query(sql_exec)
	checkErr(err)
	var row2 sql.Rows = *query2

	if reflect.DeepEqual(t_cloumn(row1), t_cloumn(row2)) == false {
		log.Debugf("字段名不同")
		panic("")
	}

	if reflect.DeepEqual(t_cloumntype(row1), t_cloumntype(row2)) == false {
		log.Debugf("列类型不同")
		panic("")
	}

	if strings.EqualFold(pri_key_dbs, pri_key_dba) == false {
		log.Debugf("主键列不一致,源表主键为：%s,目标表主键为：%s", pri_key_dbs, pri_key_dba)
		panic("")
	}
	count_int, _ := strconv.Atoi(count_dbs)
	chunk_count_remainder := count_int % chunksize
	chunk_count := chunk_table(count_int, chunk_count_remainder, chunksize)
	log.Debugf("chunk数目为:%d", chunk_count)

	//开启10个goroutine去比较md5
	for d := 0; d <= 10; d++ {
		go func() {
			for {
				srm := <-ch_md5
				chunk_checktable(srm.query_dbs_md5, srm.query_dba_md5)
			}
		}()
	}

	//开启10个goroutine去生成MD5
	for c := 0; c <= 10; c++ {
		go func() {
			for {
				sr := <-ch_result
				query_dbs_md5 := encryption(printResult(sr.query_dbs_result))
				query_dba_md5 := encryption(printResult(sr.query_dba_result))
				var srm struct_result_md5
				srm.query_dbs_md5 = query_dbs_md5
				srm.query_dba_md5 = query_dba_md5
				ch_md5 <- srm
			}
		}()
	}

	//循环执行chunk_count次并做校验
	bar := pb.StartNew(chunk_count) //校验进度条
	var j int = 0
	for i := 0; i < chunk_count; i++ {
		//考虑发号器导致的不规则连续性，第一个版本先采用主键延迟关联查询的方式
		sql_exec := "SELECT a.* FROM " + table + " a, (select " + pri_key + " from " + table + " ORDER BY " + pri_key + " limit " + strconv.Itoa(j) + "," + strconv.Itoa(chunksize) + " ) b where a." + pri_key + "=b." + pri_key
		//log.Debugf("执行的sql是%s", sql_exec)
		query_dbs_result, err := dbs.Query(sql_exec)
		checkErr(err)
		query_dba_result, err := dba.Query(sql_exec)
		checkErr(err)
		var sr struct_result
		sr.query_dbs_result = query_dbs_result
		sr.query_dba_result = query_dba_result
		ch_result <- sr
		j = j + chunksize
		bar.Increment()
		time.Sleep(time.Millisecond)
	}

	elapsed := time.Since(t1)
	log.Debugf("耗时:%s", elapsed)
}

func db_conn(shost string, sport int, ahost string, aport int, user string, password string, database string) (*sql.DB, *sql.DB) {
	var dbresource, dbaims string
	dbresource = user + ":" + password + "@" + "tcp" + "(" + shost + ":" + fmt.Sprintf("%d", sport) + ")" + "/" + database + "?charset=utf8"
	log.Debugf("源连接地址:%s", fmt.Sprintf(dbresource))
	dbs, err := sql.Open("mysql", dbresource)
	checkErr(err)

	dbaims = user + ":" + password + "@" + "tcp" + "(" + ahost + ":" + fmt.Sprintf("%d", aport) + ")" + "/" + database + "?charset=utf8"
	dba, err := sql.Open("mysql", dbaims) //第一个参数为驱动名
	checkErr(err)
	log.Debugf("目标连接地址:%s", fmt.Sprintf(dbaims))

	return dbs, dba
}

//读取主键列
func str_pri(str string) string {
	str_int1 := strings.Index(str, "BTREEPRIMARY")
	str_int2 := strings.Index(str, "A")
	return str[str_int2+1 : str_int1]
}

//生成chunk数目
func chunk_table(count_int, chunk_count_remainder, chunksize int) int {
	if chunk_count_remainder == 0 {
		var chunk_count int
		chunk_count = count_int / chunksize
		return chunk_count
	} else {
		var chunk_count int
		chunk_count = count_int/chunksize + 1
		return chunk_count
	}

}

//比对MD5值
func chunk_checktable(query_dbs_md5, query_dba_md5 string) {
	if strings.EqualFold(query_dbs_md5, query_dba_md5) == false {
		log.Debugf("数据不一致")
		panic("")
	}
}

//读出查询出的列字段名
func t_cloumn(query sql.Rows) []string {
	column, _ := query.Columns()
	return column
}

//读出查询出的列类型
func t_cloumntype(query sql.Rows) []*sql.ColumnType {
	columntype, _ := query.ColumnTypes()
	return columntype
}

//输出查询结果，无符号分割一连串的的string
func printResult(query *sql.Rows) string {
	column, _ := query.Columns()
	values := make([][]byte, len(column))     //values是每个列的值，这里获取到byte里
	scans := make([]interface{}, len(column)) //因为每次查询出来的列是不定长的，用len(column)定住当次查询的长度
	for i := range values {                   //让每一行数据都填充到[][]byte里面
		scans[i] = &values[i]
	}
	result := make([]string, 0)
	for query.Next() { //循环，让游标往下移动
		if err := query.Scan(scans...); err != nil { //query.Scan查询出来的不定长值放到scans[i] = &values[i],也就是每行都放在values里
			fmt.Println(err)
			//return
		}
		row := make(map[string]string) //每行数据
		for k, v := range values {     //每行数据是放在values里面，现在把它挪到row里
			key := column[k]
			row[key] = string(v)
		}
		//fmt.Println(row)
		sorted_keys := make([]string, 0) //map无序，重新排序
		for k, _ := range row {
			sorted_keys = append(sorted_keys, k)
		}

		sort.Strings(sorted_keys) //排序

		for _, k := range sorted_keys {
			result = append(result, row[k])
		}
	}

	str := "" //将一个chunk取到的结果放到一个string中
	for _, v := range result {

		str = str + v
	}
	return str

}

//生成md5加密
func encryption(str string) string {
	h := md5.New()
	h.Write([]byte(str))
	cipherStr := h.Sum(nil)
	return hex.EncodeToString(cipherStr)
}

//打印错误
func checkErr(errMasg error) {
	if errMasg != nil {
		panic(errMasg)
	}
}

//命令行传参
func help() (string, int, string, int, string, string, string, string, int) {
	sourceadd := flag.String("shost", "127.0.0.1", "源实例地址")
	sourceport := flag.Int("sport", 3306, "源实例端口")
	aimsadd := flag.String("ahost", "127.0.0.1", "目标实例地址")
	aimsport := flag.Int("aport", 3306, "目标实例端口")
	user := flag.String("user", "admin", "连接用户")
	password := flag.String("password", "admin", "链接密码")
	database := flag.String("database", "test", "校验的数据库")
	table := flag.String("table", "usertb", "校验表")
	chunksize := flag.Int("chunksize", 1000, "chunk大小,一次处理多少行")
	help := flag.String("help", "./go-table-checksum -shost=127.0.0.1 -sport=3306 -ahost=127.0.0.1 -aport=3306 -database=test -table=usertb -user=admin -password=admin -chunksize=1000", "示例")
	flag.Parse()
	log.Debugf("示例:%s", *help)
	return *sourceadd, *sourceport, *aimsadd, *aimsport, *user, *password, *database, *table, *chunksize
}
