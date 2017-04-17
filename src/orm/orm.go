package orm

import (
	"fmt"
	"strings"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
)

type Conf struct {
	Host     string `json:"host"`
	Port     string `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
	Database string `json:"database"`
}

var Gdb *gorm.DB

func NewDb(conf *Conf) error {
	dbUri := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8&autocommit=true",
		conf.Username, conf.Password, conf.Host, conf.Port, conf.Database)

	gDb, err := gorm.Open("mysql", dbUri)
	if err != nil {
		return err
	}
	Gdb = gDb

	return nil
}

func Close() {
	if Gdb != nil {
		defer Gdb.Close()
	}
}

func GetTables() []string {
	type Result struct {
		TableName string
	}

	var results []Result
	res := make([]string, 0, 20)
	Gdb.Raw("select table_name from information_schema.TABLES where table_schema=?", "persistent").Scan(&results)
	for _, result := range results {
		if strings.HasPrefix(result.TableName, "offer_persistent_") {
			res = append(res, result.TableName)
		}
	}
	return res
}
