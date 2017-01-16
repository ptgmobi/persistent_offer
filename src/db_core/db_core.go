// db_core  提供对快照的入库和简单的查询
package db_core

import (
	"database/sql"
	"errors"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

type Conf struct {
	Host     string `json:"host"`
	Port     string `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
	Database string `json:"database"`
}

type DBCore struct {
	//SELECT count(TABLE_NAME) FROM information_schema.TABLES WHERE TABLE_SCHEMA='persistent';
	conf      *Conf
	dbHandler *sql.DB
}

// NewDb 创建并返回一个dbservice
func NewDb(conf *Conf) (*DBCore, error) {
	// user:password@tcp(localhost:5555)/dbname?tls=skip-verify&autocommit=true
	dbUri := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8&tls=skip-verify&autocommit=true",
		conf.Username, conf.Password, conf.Host, conf.Port, conf.Database)

	handler, err := sql.Open("mysql", dbUri)
	if err != nil {
		return nil, errors.New("open mysql failed, dbUri: " + dbUri)
	}

	return &DBCore {
		conf: conf,
		dbHandler: handler,
	}, nil
}

// GetCurrentTables 获取当前数据库中的表
func (db *DBCore) GetCurrentTables() ([]string, error) {

	res := make([]string, 0, 48 * 7)
	querySQL := "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA=?;"
	rows, err := db.dbHandler.Query(querySQL, db.conf.Database)
	if err != nil {
		return res, errors.New("InitCurrentTables failed " + err.Error())
	}
	for rows.Next() {
		var tableName string
		err := rows.Scan(&tableName)
		if err != nil {
			return res, errors.New("Scan tableName err: " + err.Error())
		}
		res = append(res, tableName)
	}
	return res, nil
}

// ExecSqlQuery向指定的表中插入数据
func (db *DBCore) ExecSqlQuery(sqlQuery string) error {
	_, err := db.dbHandler.Exec(sqlQuery,)
	if err != nil {
		return err
	}
	return nil
}

func (db *DBCore) ExecSqlQueryWithParameter(sqlQuery string, args ...interface{}) error {
	fmt.Println("sql: ", sqlQuery)
	stm, err := db.dbHandler.Prepare(sqlQuery)
	defer stm.Close()
	if err != nil {
		return err
	}

	_, err = stm.Exec(args...)
	if err != nil {
		return err
	}
	return nil
}
