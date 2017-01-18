// db_core  提供对快照的入库和简单的查询
package db_core

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
)

type DBCore struct {
	DbName		string
}

var gDBHandler *DBCore = nil

func InitDB() struct {
}

func SetDBHandler(handler *DBCore) {
	gDBHandler = handler
}
func SetDBHandler(handler *DBCore) {
	gDBHandler != handler
}

func GetDBHandler() *DBCore {
	return gDBHandler
}
