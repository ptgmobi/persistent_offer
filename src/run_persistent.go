package main

import (
	"time"

	"github.com/dongjiahong/gotools"

	fs "fetch_snapshot"
	db "db_core"
)

type Conf struct {
	FetchSnapshotConf fs.Conf `json:"fetch_snapshot"`
	DbConf			  db.Conf `json:"mysql_config"`
}

var gConf Conf

func main() {
	if err := gotools.DecodeJsonFromFile("conf/persistent.conf", &gConf); err != nil {
		panic(err)
	}

	fetchService := fs.NewService(&gConf.FetchSnapshotConf, &gConf.DbConf)
	if fetchService == nil {
		panic("create fetchService failed!")
	}
	go fetchService.Server()

	// TODO SEARCH
	time.Sleep(time.Hour)
}
