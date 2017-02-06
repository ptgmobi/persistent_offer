package search

import (
	"testing"

	dbCore "db_core"
)

var s = InitServer()

func InitServer() *Service {
	conf := &Conf {
		SearchPath: "/search",
		Port: "10080",
		Host: "127.0.0.1",
		LogPath: "../../logs/search",
	}

	dbConf := &dbCore.Conf {
		Host: "127.0.0.1",
		Port: "3306",
		Username: "test",
		Password: "test",
		Database: "test",
	}

	s := NewService(conf, dbConf)
	return s
}

func Test_getNearTable(t *testing.T) {
	tables := make([]string, 0, 5)
	tables = append(tables, "a_b_201701011230")
	tables = append(tables, "a_b_201701011300")
	tables = append(tables, "a_b_201701011400")
	tables = append(tables, "a_b_201701011500")
	tables = append(tables, "a_b_201701011600")

	if (s.getNearTable("201701021300", tables) != "a_b_201701011600") {
		t.Error("getNearTable err, table: ", s.getNearTable("201701021300", tables))
	}

	if (s.getNearTable("201601021300", tables) != "") {
		t.Error("getNearTable err, table: ", s.getNearTable("201601021300", tables))
	}
}
