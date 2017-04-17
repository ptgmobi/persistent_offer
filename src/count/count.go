// 从数据库持久化表里获取数据，插入到统计库里
package count

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	fs "fetch_snapshot"
	"orm"
)

type Count struct {
	Id         string // ym_123
	UpdateDate string // 20170809
	UpdateHour string // 0-23
	Channel    string // ym|tym|adst|irs...
	Platform   string // iOS|Android
	Country    string // CN|US|JP...
}

/*
create table counts(
	id char(255) not null comment 'dnfid',
	update_date char(255) not null comment '更新的日期，年月日',
	update_hour char(255) not null comment '更新时间，小时',
	channel char(255) not null comment 'offer的渠道',
	platform char(255) not null comment 'offer的平台ios|android',
	country char(255) not null comment 'offer所属国家',
	unique key(id, update_date, update_hour, country),
	key `idx_id` (`id`)
)ENGINE=InnoDB default CHARSET=utf8;
*/

type RawData struct {
	DocId      string `gorm:"column:docid"`
	InsertDate string `gorm:"column:insertDate"`
	AdId       string `gorm:"column:adid"`
	AppPkgName string `gorm:"column:app_pkg_name"`
	Channel    string `gorm:"column:channel"`
	FinalUrl   string `gorm:"column:final_url"`
	Content    []byte `gorm:"column:content"`
}

// 根据给定的时间获取对应的持久化数据
func getReadDbName(date string) string {
	currentTables := orm.GetTables()
	matchTables := make([]string, 0, 2)
	filter := func(t string) bool {
		// offer_persistent_201702141452
		names := strings.Split(t, "_")
		if len(names) == 3 {
			// 年   月 日 时 分
			// 2017 02 14 15 52
			if date == names[2][0:10] {
				return true
			}
		}
		return false
	}

	if len(currentTables) > 0 {
		for _, table := range currentTables {
			if filter(table) {
				matchTables = append(matchTables, table)
			}
		}
	}

	if len(matchTables) > 0 {
		return matchTables[0]
	}
	return ""
}

type CFunc func(data *RawData) []*Count

// 获取表中数据
func getData(table string, cFunc CFunc) ([]*Count, error) {
	res := make([]*Count, 0, 1024)
	rawDatas := make([]RawData, 0, 1024)

	errs := orm.Gdb.Table(table).Find(&rawDatas).GetErrors()
	if len(errs) > 0 {
		for _, err := range errs {
			log.Println("[getData] err: ", err)
		}
		return nil, errs[0]
	}
	for i := 0; i < len(rawDatas); i++ {
		if record := cFunc(&rawDatas[i]); record != nil {
			res = append(res, record...)
		}
	}
	if len(res) > 0 {
		return res, nil
	}
	return nil, errors.New("can't get data")
}

// 插入新的统计表
func insertData(records []*Count) {
	for _, record := range records {
		orm.Gdb.NewRecord(record)
		errs := orm.Gdb.Create(record).GetErrors()
		if len(errs) > 0 {
			log.Println("[insertData] insert err: ", errs)
		}
	}
}

func Server() {
	// 插入数据一小时一次就好
	for {
		t := time.Now().UTC()
		date := t.Format("200601021504")
		hour := date[8:10]
		var table = getReadDbName(date[0:10])

		if table != "" {
			if records, err := getData(table, func(data *RawData) []*Count {
				raws := make([]*Count, 0, 8)
				var content fs.Offer
				err := json.Unmarshal(data.Content, &content)
				if err != nil {
					log.Println("[Serve] get content err: ", err)
					return nil
				}
				for _, country := range content.Attr.Countries {
					var c Count
					c.Channel = data.Channel
					c.UpdateDate = date
					c.UpdateHour = hour
					c.Id = data.DocId
					c.Platform = content.Attr.Platform
					c.Country = country
					raws = append(raws, &c)
				}
				return raws
			}); err != nil {
				log.Println("get data err: ", err)
			} else {
				insertData(records)
			}
		} else {
			log.Println("[Serve] can't get table name!")
		}

		log.Println("Loop count hour: ", hour)
		time.Sleep(time.Minute * 30)
	}
}

func CountWithDimension(dimension, condition, date string) (int64, error) {
	if dimension != "channel" && dimension != "platform" && dimension != "country" {
		return -1, errors.New("CountWithDimension don't support dimension: " + dimension)
	}

	if len(condition) == 0 || len(date) == 0 {
		return -1, errors.New("CountWithDimension parameter condition or date is nil")
	}

	type Result struct {
		Id string
	}
	var results []Result
	var sql string

	sql = fmt.Sprintf("select id from counts where %s='%s' and update_date='%s' group by id",
		dimension, condition, date)

	gdb := orm.Gdb.Raw(sql).Scan(&results)
	errs := gdb.GetErrors()
	if len(errs) > 0 {
		return -1, errs[0]
	}
	num := gdb.RowsAffected
	return num, nil
}
