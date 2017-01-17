package main

import (
	"github.com/dongjiahong/gotools"

	db "db_core"
	fs "fetch_snapshot"
	"search"
)

type Conf struct {
	FetchSnapshotConf fs.Conf     `json:"fetch_snapshot"`
	DbConf            db.Conf     `json:"mysql_config"`
	SearchConf        search.Conf `json:"search"`
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

	searchService := search.NewService(&gConf.SearchConf, &gConf.DbConf)
	if searchService == nil {
		panic("create searchService failed!")
	}
	searchService.StartServer()
}
