package tablechecksum

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/ngaut/log"
)

//连接源地址和目标地址实例
func DB_conn(vsource MysqlsourceConfig, vaims MysqlaimsConfig, database, table string, chunksize, goroutine int) (*sql.DB, *sql.DB) {
	sourcehost := vsource.Addr
	sourceport := vsource.Port
	sourceuser := vsource.User
	sourcepasswd := vsource.Password
	dbresource := sourceuser + ":" + sourcepasswd + "@" + "tcp" + "(" + sourcehost + ":" + sourceport + ")" + "/" + database + "?charset=utf8"
	log.Infof("源连接地址:%s", fmt.Sprintf(dbresource))
	dbs, err := sql.Open("mysql", dbresource)
	CheckErr(err)
	aimshost := vaims.Addr
	aimsport := vaims.Port
	aimsuser := vaims.User
	aimpasswd := vaims.Password
	dbaims := aimsuser + ":" + aimpasswd + "@" + "tcp" + "(" + aimshost + ":" + aimsport + ")" + "/" + database + "?charset=utf8"
	dba, err := sql.Open("mysql", dbaims)
	CheckErr(err)
	log.Infof("目标连接地址:%s", fmt.Sprintf(dbaims))

	NewTablechecksum(dbs, dba, database, table, chunksize, goroutine)
	return dbs, dba
}
func CheckErr(errMasg error) {
	if errMasg != nil {
		panic(errMasg)
	}
}
