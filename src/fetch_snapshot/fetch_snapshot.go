package fetch_snapshot

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
	"strings"
	"sort"
	"io/ioutil"
	"log"

	"github.com/dongjiahong/gotools"

	dbCore "db_core"
)

var table_prefix = "offer_persistent_"

type Conf struct {
	FetchApi       string `json:"fetch_api"`
	FetchFrequency int    `json:"fetch_frequency"` // fetch 频率（分钟）

	LogPath string `json:"log_path"`

	PersistentTime int    `json:"persistent_time"` // 数据保存的天数

}

type Service struct {
	conf *Conf
	l    *gotools.RotateLog
	db   *dbCore.DBCore
}

type Snapshot struct {
	Data         []Offer `json:"data"`
	TotalRecords int     `json:"total_records"`
}

type Offer struct {
	Active bool      `json:"active"`
	Dnf    string    `json:"dnf"`
	Docid  string    `json:"docid"`
	Name   string    `json:"name"`
	Attr   Attribute `json:"attr"`
}

type Attribute struct {
	AdExpireTime    int              `json:"ad_expire_time"`
	Adid            string           `json:"adid"`
	AppCategory     []string         `json:"app_category"`
	AppDown         AppDownload      `json:"app_download"`
	Channel         string           `json:"channel"`
	ClickCallback   string           `json:"click_callback"`
	ClkTks          []string         `json:"clk_tks"`
	ClkUrl          string           `json:"clk_url"`
	Countries       []string         `json:"countries"`
	Creatives       CreativeLanguage `json:"creatives"`
	FinalUrl        string           `json:"final_url"`
	Icons           CreativeLanguage `json:"icons"`
	LandingType     int              `json:"landing_type"`
	Payout          float32          `json:"payout"`
	Platform        string           `json:"platform"`
	ProductCategory string           `json:"product_category"`
	RenderImgs      RenderImg        `json:"render_imgs"`
	ThirdPartyClkTks []string `json:"third_party_clk_tks"`
	ThirdPartyImpTks []string `json:"third_party_imp_tks"`
}

type AppDownload struct {
	AppPkgName   string  `json:"app_pkg_name"`
	Description  string  `json:"description"`
	Download     string  `json:"download"`
	Rate         float32 `json:"rate"`
	Review       int     `json:"review"`
	Size         string  `json:"size"`
	Title        string  `json:"title"`
	TrackingLink string  `json:"tracking_link"`
}

type CreativeLanguage struct {
	ALL []Creative `json:"ALL"`
}

type Creative struct {
	Height   int    `json:"height"`
	Language string `json:"language"`
	Url      string `json:"url"`
	Width    int    `json:"width"`
}

type RenderImg struct {
	R500500  string `json:"500500"`
	R7201280 string `json:"7201280"`
	R950500  string `json:"950500"`
}

func NewService(conf *Conf, dbConf *dbCore.Conf) (*Service) {
	l, err := gotools.NewRotateLog(conf.LogPath, "", log.LUTC|log.LstdFlags)
	if err != nil {
		fmt.Println("[FetchSnapshot] create log err: ", err)
		return nil
	}

	db, err := dbCore.NewDb(dbConf)
	if err != nil {
		fmt.Println("NewService get db handler err: ", err)
		return nil
	}

	srv := &Service{
		conf: conf,
		l:    l,
		db:   db,
	}

	return srv
}

// Server 该函数准备新表,调用入库数据，并删除旧表
func (s *Service) Server() {
	for {
		s.l.Println("go fetch offer!")
		// 准备新表
		tableTime := time.Now().Format("200601021504") // 年月日时分
		tableName := table_prefix + tableTime
		createSql := s.getCreateTableSqlQuery(tableName)
		if len(createSql) == 0 {
			s.l.Println("Server getCreateTableSqlQuery failed sleep 5 minute")
			time.Sleep(time.Minute * 5)
			continue
		}
		err := s.db.ExecSqlQuery(createSql)
		if err != nil {
			s.l.Println("Server CreatTable err: ", err, " sqlQuery: ", createSql, " will sleep 5 minute")
			time.Sleep(time.Minute * 5)
			continue
		}
		// 入库
		s.fetchSnapshot(tableName)
		// 删除旧表
		tables, err := s.db.GetCurrentTables()
		if err != nil {
			s.l.Println("Server GetCurrentTables err: ", err, " will sleep 5 minute")
			time.Sleep(time.Minute * 5)
			continue
		}

		tablesName := checkTableName(tables)
		if len(tablesName) == 0 {
			s.l.Println("Server checkTableName  tables is nil")
		}
		s.deleteOldTable(tablesName)
		time.Sleep(time.Duration(s.conf.FetchFrequency) * time.Minute)
	}
}

func checkTableName(tables []string) []string {
	res := make([]string, 0, 48 * 7) // 24 * 2 * 7 默认半小时一次，共7天
	for t := range tables {
		if strings.HasPrefix(t, table_prefix) {
			res = append(res, t)
		}
	}
	return res
}

func (s *Service) deleteOldTable(tables []string) {
	oneDayMinutes := 24 * 60                                // 一天多少分钟
	oneDayTableNum := oneDayMinutes / s.conf.FetchFrequency // 一天会有多少表
	totalTableNum := oneDayTableNum * s.conf.PersistentTime // 数据保留期一共有多少表
	if len(tables) <= totalTableNum {                      // 表很少不删除旧表
		return
	} else {
		sort.Strings(tables)
		sqlQuery := s.getDeleteTableSqlQuery(tables[0])
		err := s.db.ExecSqlQuery(sqlQuery)
		if err != nil {
			s.l.Println("deleteOldTable delete err: ", err, " sqlQuery: ", sqlQuery)
		}
		s.l.Println("deleteOldTable delete tableName: ", tables[0])
	}
}

func (s *Service) getCreateTableSqlQuery(tableName string) string {
	sqlQuery := fmt.Sprintf(
	`create table %s(
		docid char(255) not null comment '主键dnfid',
		insertDate char(255) not null comment '插入记录时的时间',
		active char(255) not null comment 'offer是否有效',
		ad_expire_time int default 1000 comment 'ad的有效时间',
		adid char(255) not null comment 'offer id',
		app_category varchar(1000) comment 'app分类逗号分隔',
		app_pkg_name char(255) comment 'app包名',
		description text comment 'app的描述',
		download char(255) comment 'app的下载量',
		rate float comment 'app评分',
		review int comment '评论数',
		app_size char(255) comment 'app安装包大小',
		title varchar(1000) comment 'app的title',
		channel char(255) not null comment '渠道',
		click_callback varchar(1000),
		clk_tks varchar(1000),
		countries varchar(1000) comment 'app投放的国家',
		creatives json comment 'app的创意',
		final_url varchar(1000) comment '最终的app商店链接',
		icons json comment 'app的icon',
		landing_type int,
		payout float not null comment 'offer的单价',
		platform char(255) not null comment 'app投放的平台',
		product_category char(255) default 'googleplaydownload',
		render_imgs json comment 'render 图片',
		third_party_clk_tks text,
		third_party_imp_tks text,
		dnf varchar(1000) not null comment 'dnf的查询条件',
		name char(255),
		PRIMARY key(docid),
		key idx_adid (adid)
	)ENGINE=InnoDB default CHARSET=utf8;` , tableName)
	return sqlQuery
}

func (s *Service) getDeleteTableSqlQuery(tableName string) string {
	if len(tableName) == 0 {
		s.l.Println("getDeleteTableSqlQuery tableName is nil")
		return ""
	}
	query := fmt.Sprintf("drop table %s;", tableName)
	return query
}

func (s *Service) fetchSnapshot(tableName string) error {
	if len(s.conf.FetchApi) == 0 || len(tableName) == 0 {
		s.l.Println(" >>>>> FetchApi is nil or tableName is nil, api: ", s.conf.FetchApi,
			" tableName: ", tableName)
		return nil
	}
	// https://api.cloudmobi.net:9992/dump?page_size=1000&page_num=

	var pageNum = 0
	snapShotOfferCnt := 0
	for {
		pageNum++
		if pageNum == 100 {
			s.l.Println("[Warn] FetchSnapshot pageNum >= 100 !")
		}

		uri := fmt.Sprintf("%s%d", s.conf.FetchApi, pageNum)
		s.l.Println("FetchSnapshot FetchApi: ", uri)
		resp, err := http.Get(uri)
		if resp != nil {
			defer resp.Body.Close()
		}
		if err != nil {
			return err
		}
		var snapshot Snapshot
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			s.l.Println("[Warn] FetchSnapshot read body err: ", err)
			continue
		}
		err = json.Unmarshal(body, &snapshot)
		if err != nil {
			s.l.Println("[Warn] FetchSnapshot unmarshal err: ", err)
		}
		if len(snapshot.Data) == 0 {
			s.l.Println("FetchSnapshot over, snapShotOfferCnt: ", snapShotOfferCnt)
			break
		}

		for i := 0; i < len(snapshot.Data); i++ {
			query :=  "insert into "+tableName+" values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
			if len(sqlQuery) == 0 {
				s.l.Println("FetchSnapshot getInsertSqlQuery failed, sqlQuery: ", sqlQuery)
			} else {
				offer := snapshot.Data[i]
				creativesJson, err := json.Marshal(offer.Attr.Creatives)
				if err != nil {
					s.l.Println("getInsertSqlQuery marshal creatives err: ", err)
				}

				iconJson, err := json.Marshal(offer.Attr.Icons)
				if err != nil {
					s.l.Println("getInsertSqlQuery marshal icon err: ", err)
				}

				renderJson, err := json.Marshal(offer.Attr.RenderImgs)
				if err != nil {
					s.l.Println("getInsertSqlQuery marshal renderimg err: ", err)
				}

				contentJson, err := json.Marshal(offer)
				if err != nil {
					s.l.Println("getInsertSqlQuery marshal contentJson err: ", err)
				}
				err = s.db.ExecSqlQueryWithParameter(sqlQuery,
				offer.Docid,
				time.Now().Format("200601021504"),
				fmt.Sprintf("%v",offer.Active),
				offer.Attr.AdExpireTime,
				offer.Attr.Adid,
				strings.Join(offer.Attr.AppCategory, ","),
				offer.Attr.AppDown.AppPkgName,
				offer.Attr.AppDown.Description,
				offer.Attr.AppDown.Download,
				offer.Attr.AppDown.Rate,
				offer.Attr.AppDown.Review,
				offer.Attr.AppDown.Size,
				offer.Attr.AppDown.Title,
				offer.Attr.Channel,
				offer.Attr.ClickCallback,
				strings.Join(offer.Attr.ClkTks, ";"),
				strings.Join(offer.Attr.Countries, ","),
				string(creativesJson),
				offer.Attr.FinalUrl,
				string(iconJson),
				offer.Attr.LandingType,
				offer.Attr.Payout,
				offer.Attr.Platform,
				offer.Attr.ProductCategory,
				string(renderJson),
				strings.Join(offer.Attr.ThirdPartyClkTks, ";"),
				strings.Join(offer.Attr.ThirdPartyImpTks, ";"),
				offer.Dnf,
				offer.Name)
				if err != nil {
					s.l.Println("FetchSnapshot insertToTable err: ", err)
				}
				snapShotOfferCnt++
				s.l.Println("FetchSnapshot insert records success, cnt: ", snapShotOfferCnt)
			}
		}

		currentTotalRecords := pageNum * 1000
		if snapshot.TotalRecords > 0 && currentTotalRecords >= snapshot.TotalRecords {
			s.l.Println("FetchSnapshot fetch over, currentTotalRecords: ", currentTotalRecords,
				" TotalRecords: ", snapshot.TotalRecords)
			break
		}
	}

	if snapShotOfferCnt <= 0 {
		return errors.New(fmt.Sprintf("snapShotOfferCnt: %d", snapShotOfferCnt))
	}

	s.l.Println("fetchSnapshot ok!")
	return nil
}
