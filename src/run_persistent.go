package main

import (
	"github.com/dongjiahong/gotools"

	"count"
	db "db_core"
	fs "fetch_snapshot"
	orm "orm"
	"search"
)

type Conf struct {
	FetchSnapshotConf fs.Conf     `json:"fetch_snapshot"`
	DbConf            db.Conf     `json:"mysql_config"`
	SearchConf        search.Conf `json:"search"`
	OrmConf           orm.Conf    `json:"orm_config"`
}

var gConf Conf

func main() {
	if err := gotools.DecodeJsonFromFile("conf/persistent.conf", &gConf); err != nil {
		panic(err)
	}

	if err := orm.NewDb(&gConf.OrmConf); err != nil {
		panic("new mysql orm err: " + err.Error())
	}
	defer orm.Close()

	go count.Server()

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
