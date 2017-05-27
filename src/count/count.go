// 从数据库持久化表里获取数据，插入到统计库里
package count

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	fs "fetch_snapshot"
	"orm"
)

type Count struct {
	UpdateDate int    // 20170809
	Channel    string // ym|tym|adst|irs...
	Platform   string // iOS|Android
	Countries  []string
	PkgName    string // api.com
	Video      bool   // 是否有视频素材
}

type OfferRecord struct {
	Id            int64
	UpdateDate    int
	Channel       string
	Country       string
	Platform      string
	VideoOfferNum int // video的offer数量
	VideoPkgNum   int // video的包名数量
	OfferNum      int // offer总数
	PkgNum        int // 包名总数
}

/*
CREATE TABLE `offer_records` (
	`id` int(20) NOT NULL AUTO_INCREMENT,
	`update_date` int(11) NOT NULL COMMENT '更新日期',
	`channel` char(64) NOT NULL COMMENT 'offer渠道',
	`country` char(32) NOT NULL COMMENT '国家字母简写',
	`platform` char(64) NOT NULL COMMENT 'iOS or Android',
	`video_offer_num` int(11) DEFAULT '0' COMMENT '视频数',
	`video_pkg_num` int(11) DEFAULT '0' COMMENT '视频包名数',
	`offer_num` int(11) DEFAULT '0' COMMENT 'offer总数',
	`pkg_num` int(11) DEFAULT '0' COMMENT 'offer包数',
	PRIMARY KEY (`id`),
	KEY `update_date` (`update_date`),
	KEY `channel` (`channel`),
	KEY `country` (`country`),
	KEY `platform` (`platform`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
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

type CFunc func(data *RawData) *Count

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
			res = append(res, record)
		}
	}
	if len(res) > 0 {
		return res, nil
	}
	return nil, errors.New("can't get data")
}

func adjustRecords(records []*Count) []*OfferRecord {
	res := make([]*OfferRecord, 0, 2048)
	// 整理数据
	adjust := make(map[string][]*Count, 2048) // [ym_ios_cn][*count, *count ...]
	for _, record := range records {
		keySliceAll := make([]string, 0, 3)
		keySliceAll = append(keySliceAll, record.Channel)
		keySliceAll = append(keySliceAll, record.Platform)
		keySliceAll = append(keySliceAll, "ALL")
		keyAll := strings.Join(keySliceAll, "_")
		adjust[keyAll] = append(adjust[keyAll], record)

		for _, country := range record.Countries {
			keySlice := make([]string, 0, 3)
			keySlice = append(keySlice, record.Channel)
			keySlice = append(keySlice, record.Platform)
			keySlice = append(keySlice, country)
			key := strings.Join(keySlice, "_")

			adjust[key] = append(adjust[key], record)
		}
	}
	// 统计计数
	for k, counts := range adjust {
		var ch, plt, co string
		if keySlice := strings.Split(k, "_"); len(keySlice) != 3 {
			log.Println("[adjustRecords] split key err, key: ", k)
			continue
		} else {

			ch = keySlice[0]
			plt = keySlice[1]
			co = keySlice[2]
		}

		var offerNum, videoNum int
		pkgNum := make(map[string]bool, 128)
		videoPkgNum := make(map[string]bool, 128)

		offerNum = len(counts)
		for _, c := range counts {
			if c.Video {
				videoNum += 1
				if len(c.PkgName) > 0 {
					videoPkgNum[c.PkgName] = true
				}
			}
			if len(c.PkgName) > 0 {
				pkgNum[c.PkgName] = true
			}
		}
		or := &OfferRecord{
			UpdateDate:    counts[0].UpdateDate,
			Channel:       ch,
			Country:       co,
			Platform:      plt,
			VideoOfferNum: videoNum,
			VideoPkgNum:   len(videoPkgNum),
			OfferNum:      offerNum,
			PkgNum:        len(pkgNum),
		}
		res = append(res, or)
	}
	return res
}

// 插入新的统计表
func insertData(records []*Count) {
	ors := adjustRecords(records)
	for _, or := range ors {
		orm.Gdb.NewRecord(or)
		errs := orm.Gdb.Create(or).GetErrors()
		if len(errs) > 0 {
			log.Println("[insertData] insert offer records err: ", errs)
		}
	}

}

func delOldData() {
	old := time.Now().UTC().AddDate(0, 0, -31).Format("2006010215") // 删除3周前的
	//old := time.Now().Format("2006010215") // 删除3周前的
	date, err := strconv.Atoi(old)
	if err != nil {
		log.Println("[delOldData] get date err: ", err)
		return
	}
	x := orm.Gdb.Delete(OfferRecord{}, "update_date < ?", date)
	errs := x.GetErrors()
	if len(errs) > 0 {
		log.Println("[delOldData] delete old data err: ", errs)
	}
	log.Println("[delOldData] delete records num: ", x.RowsAffected)
}

func Server() {
	// 插入数据一小时一次就好
	for {
		t := time.Now().UTC()
		date := t.Add(time.Hour * -1).Format("2006010215")
		hour := date[8:10]

		var table = getReadDbName(date)
		log.Println("-----------> table: ", table)

		if table != "" {
			if records, err := getData(table, func(data *RawData) *Count {
				var content fs.Offer
				err := json.Unmarshal(data.Content, &content)
				if err != nil {
					log.Println("[Serve] get content err: ", err)
					return nil
				}

				var c Count
				c.Channel = data.Channel
				dateInt, _ := strconv.Atoi(date)
				c.UpdateDate = dateInt
				c.Platform = content.Attr.Platform
				c.Countries = append(c.Countries, content.Attr.Countries...)
				c.PkgName = content.Attr.AppDown.AppPkgName
				if strings.Contains(content.Dnf, "video,") {
					c.Video = true
				}

				return &c
			}); err != nil {
				log.Println("get data err: ", err)
			} else {
				insertData(records)
				delOldData()
			}
		} else {
			log.Println("[Serve] can't get table name!")
		}

		log.Println("Loop count hour: ", hour)
		time.Sleep(time.Minute * 60)
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
