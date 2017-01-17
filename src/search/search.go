// inquire 提供简单的offer快照查询功能
package search

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/dongjiahong/gotools"
	_ "github.com/go-sql-driver/mysql"

	dbCore "db_core"
	fs "fetch_snapshot"
)

type Conf struct {
	SearchPath string `json:"search_path"`
	Port       string `json:"port"`
	Host       string `json:"host"`
	LogPath    string `json:"log_path"`
}

type Service struct {
	conf *Conf
	l    *gotools.RotateLog
	db   *dbCore.DBCore
}

type WrapOffer struct {
	InsertTime string   `json:"record_time"`
	Offer      fs.Offer `json:"offer"`
}

type ResultData struct {
	Msg    string      `json:"message"`
	Status bool        `json:"status"`
	Data   []WrapOffer `json:"snapshots"`
}

func NewService(conf *Conf, dbConf *dbCore.Conf) *Service {
	l, err := gotools.NewRotateLog(conf.LogPath, "", log.LUTC|log.LstdFlags)
	if err != nil {
		fmt.Println("[NewService] create log err: ", err)
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

func (s *Service) wrapResultData(msg string, status bool, data []WrapOffer) (string, error) {
	var rd = ResultData{
		Msg:    msg,
		Status: status,
		Data:   data,
	}
	res, err := json.Marshal(&rd)
	if err != nil {
		s.l.Println("[Warn] wrapResultData marshal data err: ", err)
		rd.Msg = "get data failed!"
		rd.Status = false
		rd.Data = nil
		res, _ = json.Marshal(&rd)
		return string(res), errors.New("wrapResultData marshal data err: " + err.Error())
	}
	return string(res), nil
}

// checkoutParmeter 检查参数，如果参数有问题并返回msg
func (s *Service) checkoutParmeter(form url.Values) (bool, string) {
	docid := form.Get("docid")
	offerid := form.Get("offerid")
	time := form.Get("time")

	if len(docid) == 0 && len(offerid) == 0 {
		return false, "need docid or offerid"
	}
	if len(docid) != 0 {
		two := strings.Split(docid, "_") // nglp_12345
		if len(two) != 2 {
			return false, "check docid format, example: ym_12345"
		}
	}

	if len(time) != 0 {
		// 时间只要分钟级别 如：201701161330
		if len(time) < 12 {
			return false, "check the time parmeter, example: 101701161230"
		}
	}
	return true, ""
}

func (s *Service) HandlerSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-type", "application/json")
	r.ParseForm()
	check, msg := s.checkoutParmeter(r.Form)
	if !check {
		res, _ := s.wrapResultData(msg, false, nil)
		w.Write([]byte(res))
		return
	}
	// http://127.0.0.1:10080/persistent/search?time=&offerid=
	// http://127.0.0.1:10080/persistent/search?time=&dnfid=
	var res string
	time := r.Form.Get("time")
	time = time[:12] //
	docid := r.Form.Get("docid")
	offerid := r.Form.Get("offerid")

	msg, data := s.getSnapshot(time, docid, offerid)
	res, err := s.wrapResultData(msg, true, data)
	if err != nil {
		s.l.Println(err.Error(), " docid: ", docid, " offerid: ", offerid, " time: ", time)
	}
	w.Write([]byte(res))
	return
}

// 从数据库中获取offer
func (s *Service) getSnapshot(time string, docid string, offerid string) (string, []WrapOffer) {
	// time = 201701161732
	currentTables, err := s.db.GetCurrentTables(fs.Table_prefix)
	res := make([]WrapOffer, 0, 8)
	if err != nil {
		s.l.Println("[Warn] GetCurrentTables err: ", err)
		return "get table info err", nil
	}
	s.l.Println("currentTables tables: ", currentTables)
	if len(time) == 0 { // 没有传时间将会把所有表里的结果返回
		// TODO
		return "need time parameter", nil
	} else { // 获取给定时间最近的上一个时间节点的表
		nearTable := s.getNearTable(time, currentTables)
		if len(nearTable) == 0 {
			return "can't find data whith this time", nil
		}

		var sqlQuery string
		if len(offerid) == 0 {
			sqlQuery = "select insertDate, content from " + nearTable + " where docid='" + docid + "';"
		}
		if len(docid) == 0 {
			sqlQuery = "select insertDate, content from " + nearTable + " where adid='" + offerid + "';"
		}

		rows, err := s.db.GetDataWithRows(sqlQuery)
		defer rows.Close()
		if err != nil {
			s.l.Println("[Warn] getSnapshot query failed: ", err, " sqlQuery: ", sqlQuery)
			return "query db failed", nil
		}

		for rows.Next() {
			var insertDate string
			var offerStr string
			var offer fs.Offer
			if err := rows.Scan(&insertDate, &offerStr); err != nil {
				s.l.Println("[Warn] Scan rows err: ", err, " sqlQuery: ", sqlQuery)
				continue
			}
			err := json.Unmarshal([]byte(offerStr), &offer)
			if err != nil {
				s.l.Println("[Warn] Unmarshal offer err: ", err, " row: ", offerStr)
				continue
			}
			var wrapOffer = WrapOffer{
				InsertTime: insertDate,
				Offer:      offer,
			}
			res = append(res, wrapOffer)
		}
		if len(res) != 0 {
			return "offer is valid", res
		} else {
			return "offer is invalid", nil
		}
	}
}

func (s *Service) getNearTable(time string, tables []string) string {
	var min int
	var resTable string
	timeInt, _ := strconv.Atoi(time)
	sort.Strings(tables)
	for i := 0; i < len(tables); i++ {
		date := strings.Split(tables[i], "_")
		if len(date) != 3 {
			s.l.Println("the tableName wrong, name: ", tables[i])
			continue
		}
		dateInt, _ := strconv.Atoi(date[2])
		if i == 0 {
			if timeInt < dateInt {
				return resTable // 时间点太靠前，没有对应表
			}
			min = timeInt - dateInt
			resTable = tables[i]
		}
		tmp := timeInt - dateInt
		if tmp > 0 && tmp < min {
			min = tmp
			resTable = tables[i]
		}
	}
	return resTable
}

func (s *Service) StartServer() {
	http.HandleFunc(s.conf.SearchPath, s.HandlerSearch)

	panic(http.ListenAndServe(s.conf.Host+":"+s.conf.Port, nil))
}
