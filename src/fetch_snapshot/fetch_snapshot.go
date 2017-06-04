package fetch_snapshot

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/dongjiahong/gotools"

	dbCore "db_core"
)

var Table_prefix = "offer_persistent_"

type Conf struct {
	FetchApi       string   `json:"fetch_api"`
	FetchFrequency int      `json:"fetch_frequency"` // fetch 频率（分钟）
	FetchIpApi     []string `json:"direct_ip_api"`

	LogPath string `json:"log_path"`

	PersistentTime int `json:"persistent_time"` // 数据保存的天数
}

type Service struct {
	conf *Conf
	l    *gotools.RotateLog
	db   *dbCore.DBCore

	domain_api string
	apiLock    sync.Mutex
}

type Snapshot struct {
	Data         []Offer `json:"data"`
	TotalRecords int     `json:"total_records"`
}

type Offer struct {
	Active  bool      `json:"active"`
	Comment string    `json:"comment,omitempty"`
	Dnf     string    `json:"dnf"`
	Docid   string    `json:"docid"`
	Name    string    `json:"name"`
	Attr    Attribute `json:"attr"`
}

type Attribute struct {
	AdExpireTime     int              `json:"ad_expire_time"`
	Adid             string           `json:"adid"`
	AppCategory      []string         `json:"app_category"`
	AppDown          AppDownload      `json:"app_download"`
	Channel          string           `json:"channel"`
	ClickCallback    string           `json:"click_callback"`
	ClkTks           []string         `json:"clk_tks"`
	ClkUrl           string           `json:"clk_url"`
	Countries        []string         `json:"countries"`
	Creatives        CreativeLanguage `json:"-"`
	FinalUrl         string           `json:"final_url"`
	Icons            CreativeLanguage `json:"-"`
	LandingType      int              `json:"landing_type"`
	Payout           float32          `json:"payout"`
	Platform         string           `json:"platform"`
	ProductCategory  string           `json:"product_category"`
	RenderImgs       RenderImg        `json:"-"`
	ThirdPartyClkTks []string         `json:"third_party_clk_tks"`
	ThirdPartyImpTks []string         `json:"third_party_imp_tks"`
}

type AppDownload struct {
	AppPkgName   string  `json:"app_pkg_name"`
	Description  string  `json:"-"`
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

func NewService(conf *Conf, dbConf *dbCore.Conf) *Service {
	l, err := gotools.NewRotateLog(conf.LogPath, "", log.LUTC|log.LstdFlags)
	if err != nil {
		fmt.Println("[FetchSnapshot] create log err: ", err)
		return nil
	}
	l.RotateWithTime()

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

	s.apiLock.Lock()
	s.domain_api = s.conf.FetchApi // 保存domain形式的api
	s.apiLock.Unlock()

	go s.checkIpApi()

	for {
		s.l.Println("go fetch offer!")
		// 准备新表
		tableTime := time.Now().Format("200601021504") // 年月日时分
		tableName := Table_prefix + tableTime
		createSql := s.getCreateTableSqlQuery(tableName)
		if len(createSql) == 0 {
			s.l.Println("[Warn] Server getCreateTableSqlQuery failed sleep 5 minute")
			time.Sleep(time.Minute * 5)
			continue
		}
		err := s.db.ExecSqlQuery(createSql)
		if err != nil {
			s.l.Println("[Warn] Server CreatTable err: ", err, " sqlQuery: ", createSql, " will sleep 5 minute")
			time.Sleep(time.Minute * 5)
			continue
		}
		// 入库
		s.fetchSnapshot(tableName)
		// 删除旧表
		tables, err := s.db.GetCurrentTables(Table_prefix)
		if err != nil {
			s.l.Println("[Warn] Server GetCurrentTables err: ", err, " will sleep 5 minute")
			time.Sleep(time.Minute * 5)
			continue
		}

		s.deleteOldTable(tables)
		time.Sleep(time.Duration(s.conf.FetchFrequency) * time.Minute)
	}
}

// checkIpApi 用来判断使用ip直接请求的数据的链接是否有效，如果无效则使用域名进行访问
// 主要是解决域名访问会造成一定上数据不一致问题！
func (s *Service) checkIpApi() {
	if len(s.conf.FetchIpApi) == 0 {
		s.l.Println("checkIpApi can't find ipApi")
		os.Exit(1)
	}
	for {
		for apiNum := 0; apiNum < len(s.conf.FetchIpApi); apiNum++ {
			uri := fmt.Sprintf("%s%d", s.conf.FetchIpApi[apiNum], 1)
			s.l.Println("checkIpApi uri: ", uri)

			f := func(uri string) bool {
				resp, err := http.Get(uri)
				if resp != nil {
					defer resp.Body.Close()
				}
				if err != nil {
					if (apiNum + 1) == len(s.conf.FetchIpApi) { // 最后一个ipapi也不行还是用域名吧！
						s.apiLock.Lock()
						s.conf.FetchApi = s.domain_api
						s.apiLock.Unlock()
						s.l.Println("checkIpApi ipapi don't work use domain api")
						return true
					}
					return false
				}
				if resp.StatusCode == 200 {
					s.apiLock.Lock()
					s.conf.FetchApi = s.conf.FetchIpApi[apiNum]
					s.apiLock.Unlock()
					s.l.Println("checkIpApi use ipapi: ", s.conf.FetchIpApi[apiNum])
				}
				return true
			}

			if ok := f(uri); ok {
				break
			}
		}

		time.Sleep(time.Minute * 5) // 每5分钟测试一次
	}
}

func (s *Service) deleteOldTable(tables []string) {
	oneDayMinutes := 24 * 60                                // 一天多少分钟
	oneDayTableNum := oneDayMinutes / s.conf.FetchFrequency // 一天会有多少表
	totalTableNum := oneDayTableNum * s.conf.PersistentTime // 数据保留期一共有多少表
	if len(tables) <= totalTableNum {                       // 表很少不删除旧表
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
		adid char(255) not null comment 'offer id',
		app_pkg_name char(255) comment 'app包名',
		channel char(255) not null comment '渠道',
		final_url varchar(512) comment '最终的app商店链接',
		content json,
		PRIMARY key(docid),
		key idx_adid (adid)
	)ENGINE=InnoDB default CHARSET=utf8;`, tableName)
	return sqlQuery
}

func (s *Service) getDeleteTableSqlQuery(tableName string) string {
	if len(tableName) == 0 {
		s.l.Println("[Warn] getDeleteTableSqlQuery tableName is nil")
		return ""
	}
	query := fmt.Sprintf("drop table %s;", tableName)
	return query
}

func (s *Service) fetchSnapshot(tableName string) error {
	s.apiLock.Lock()
	fetchApi := s.conf.FetchApi
	s.apiLock.Unlock()

	if len(s.conf.FetchApi) == 0 || len(tableName) == 0 {
		s.l.Println(" >>>>> FetchApi is nil or tableName is nil, api: ", s.conf.FetchApi,
			" tableName: ", tableName)
		return nil
	}
	// https://api.cloudmobi.net:9992/dump?page_size=1000&page_num=

	over := false
	ch := make(chan int, 10)
	defer close(ch)

	snapShotOfferCnt := 0

	var wg sync.WaitGroup

	go func() {
		for offerCnt := range ch {
			snapShotOfferCnt += offerCnt
		}
	}()

	pageNum := 0
	for { // 1000 * 500 = 50W offer
		if over || pageNum >= 500 {
			break
		}

		for i := 0; i < 10; i++ { // 一次起 20个协程
			pageNum++
			if over || pageNum >= 500 {
				break
			}
			wg.Add(1)
			go func(page int) {
				defer wg.Done()

				uri := fmt.Sprintf("%s%d", fetchApi, page)
				s.l.Println("FetchSnapshot FetchApi: ", uri)
				resp, err := http.Get(uri)
				if resp != nil {
					defer resp.Body.Close()
				}
				if err != nil {
					over = true
					s.l.Println("[Warn] FetchSnapshot get offer server err: ", err)
					return
				}

				var snapshot Snapshot
				body, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					over = true
					s.l.Println("[Warn] FetchSnapshot read body err: ", err)
					return
				}
				err = json.Unmarshal(body, &snapshot)
				if err != nil {
					over = true
					s.l.Println("[Warn] FetchSnapshot unmarshal err: ", err)
					return
				}
				if len(snapshot.Data) == 0 {
					over = true
					s.l.Println("[Warn] FetchSnapshot over!!!")
					return
				}

				var offerCnt = 0
				for i := 0; i < len(snapshot.Data); i++ {
					sqlQuery := "insert into " + tableName + " values(?,?,?,?,?,?,?)"

					offer := snapshot.Data[i]
					contentJson, err := json.Marshal(offer)
					if err != nil {
						s.l.Println("[Warn] getInsertSqlQuery marshal contentJson err: ", err)
						continue
					}

					err = s.db.ExecSqlQueryWithParameter(sqlQuery,
						offer.Docid,
						time.Now().Format("200601021504"),
						offer.Attr.Adid,
						offer.Attr.AppDown.AppPkgName,
						offer.Attr.Channel,
						offer.Attr.FinalUrl,
						contentJson)
					if err != nil {
						s.l.Println("[Warn] FetchSnapshot insertToTable err: ", err)
						continue
					}
					offerCnt++
					s.l.Println("FetchSnapshot insert records success, cnt: ", offerCnt)
				}
				ch <- offerCnt

				currentTotalRecords := page * 1000
				if snapshot.TotalRecords > 0 && currentTotalRecords >= snapshot.TotalRecords {
					s.l.Println("FetchSnapshot fetch over, currentTotalRecords: ", currentTotalRecords,
						" TotalRecords: ", snapshot.TotalRecords)
					over = true
					return
				}
			}(pageNum)
		}
		wg.Wait()
	}

	if snapShotOfferCnt <= 0 {
		return errors.New(fmt.Sprintf("snapShotOfferCnt: %d", snapShotOfferCnt))
	}

	s.l.Println("fetchSnapshot ok!")
	return nil
}
