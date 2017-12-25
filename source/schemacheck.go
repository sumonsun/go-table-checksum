package tablechecksum

import (
	"database/sql"
	"strings"
)

//获取主键，只支持int类型有排序的的主键
func Primiarykey_get(str string) string {
	str_int1 := strings.Index(str, "BTREEPRIMARY")
	str_int2 := strings.Index(str, "A")
	return str[str_int2+1 : str_int1]
}

//读出查询出的列字段名
func Cloumn_get(query sql.Rows) []string {
	column, _ := query.Columns()
	return column
}

//读出查询出的列类型
func Cloumntype_get(query sql.Rows) []*sql.ColumnType {
	columntype, _ := query.ColumnTypes()
	return columntype
}
