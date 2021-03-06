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

	"count"
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
	InsertTime string    `json:"record_time"`
	Offer      *fs.Offer `json:"offer,omitempty"`
}

type ResultData struct {
	Msg    string      `json:"message"`
	Status bool        `json:"status"`
	Data   []WrapOffer `json:"snapshots"`
}

type WrapCount struct {
	Num    int64  `json:"offer_num"`
	ErrMsg string `json:"err_msg"`
}

func NewService(conf *Conf, dbConf *dbCore.Conf) *Service {
	l, err := gotools.NewRotateLog(conf.LogPath, "", log.LUTC|log.LstdFlags)
	if err != nil {
		fmt.Println("[NewService] create log err: ", err)
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
	title := form.Get("title")
	time := form.Get("time")
	begin := form.Get("begin")
	end := form.Get("end")

	if len(docid) == 0 && len(offerid) == 0 && len(title) == 0 {
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
			return false, "check the time parameter, example: 101701161230"
		}
	}
	if len(begin) > 0 {
		if beginInt, err := strconv.Atoi(begin); err != nil || beginInt < 0 {
			return false, "check the begin parameter, example: 1"
		}
	}
	if len(end) > 0 {
		if endInt, err := strconv.Atoi(end); err != nil || endInt < 0 {
			return false, "check the end parameter, example: 2"
		}
	}

	return true, ""
}

func (s *Service) HandlerSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-type", "application/json; charset=utf-8")
	r.ParseForm()
	check, msg := s.checkoutParmeter(r.Form)
	if !check {
		res, _ := s.wrapResultData(msg, false, nil)
		w.Write([]byte(res))
		return
	}
	// http://127.0.0.1:10080/persistent/search?time=&offerid=
	// http://127.0.0.1:10080/persistent/search?time=&docid=
	var res string
	time := r.Form.Get("time")
	if len(time) > 0 {
		time = time[:12] // 只保留到分钟级别
	}
	docid := r.Form.Get("docid")
	offerid := r.Form.Get("offerid")
	title := r.Form.Get("title")
	begin := r.Form.Get("begin")
	end := r.Form.Get("end")
	if len(begin) == 0 || len(end) == 0 {
		begin = "0"
		end = "50"
	}

	msg, data := s.getSnapshot(time, docid, offerid, title, begin, end)
	res, err := s.wrapResultData(msg, true, data)
	if err != nil {
		s.l.Println(err.Error(), " docid: ", docid, " offerid: ", offerid, " time: ", time)
	}
	w.Write([]byte(res))
	return
}

// 从数据库中获取offer
func (s *Service) getSnapshot(time, docid, offerid, title, begin, end string) (string, []WrapOffer) {
	// time = 201701161732
	currentTables, err := s.db.GetCurrentTables(fs.Table_prefix)
	if err != nil {
		s.l.Println("[Warn] GetCurrentTables err: ", err)
		return "get table info err", nil
	}
	s.l.Println("currentTables tables size: ", len(currentTables))
	var allRes []WrapOffer
	if len(time) == 0 { // 没有传时间将会把所有表里的结果返回
		sort.Strings(currentTables)
		for i := 0; i < len(currentTables); i++ {
			var sqlQuery string
			if len(offerid) != 0 {
				sqlQuery = "select insertDate, content from " + currentTables[i] + " where adid='" + offerid + "';"
			} else if len(docid) != 0 {
				sqlQuery = "select insertDate, content from " + currentTables[i] + " where docid='" + docid + "';"
			} else {
				s.l.Println("on condition can be used!")
			}
			errInfo, res := s.queryDb(sqlQuery, true)
			if len(errInfo) != 0 {
				s.l.Println("[Warn] getSnapshot get offer err: ", errInfo)
			}
			allRes = append(allRes, res...)
		}

	} else { // 如果给定时间，返回距离给定时间最近的上一次出现的结果
		nearTables := s.getNearTable(time, currentTables)
		if len(nearTables) == 0 {
			return "can't find data whith this time", nil
		}

		for i := len(nearTables) - 1; i >= 0; i-- {
			nearTable := nearTables[i]
			var sqlQuery string
			if len(offerid) != 0 {
				sqlQuery = "select insertDate, content from " + nearTable + " where adid='" + offerid + "';"
			} else if len(docid) != 0 {
				sqlQuery = "select insertDate, content from " + nearTable + " where docid='" + docid + "';"
			} else if len(title) != 0 {
				new_tbl_time := 201707110715
				t, err := strconv.Atoi(time)
				if err != nil {
					s.l.Println("time Atoi error:", err)
				}
				if t < new_tbl_time {
					sqlQuery = "select insertDate, content from " + nearTable + " where content like '%" + title + "%' limit " + begin + "," + end + ";"
				} else if nearTable > "offer_persistent_"+strconv.Itoa(new_tbl_time) {
					sqlQuery = "select insertDate, content from " + nearTable + " where title like '%" + title + "%' limit " + begin + "," + end + ";"
				} else {
					continue
				}
			} else {
				s.l.Println("on condition can be used, on time")
			}

			errInfo, res := s.queryDb(sqlQuery, false)
			if len(errInfo) != 0 {
				s.l.Println("[Warn] getSnapshot get offer err: ", errInfo)
			} else {
				if len(res) > 0 {
					allRes = append(allRes, res...)
					break
				}
			}
		}
	}

	if len(allRes) > 0 {
		return "offer is valid", allRes
	} else {
		return "offer is invalid", nil
	}
}

func (s *Service) queryDb(sqlQuery string, sketch bool) (string, []WrapOffer) {
	if len(sqlQuery) == 0 {
		return "[Warn] queryDb sqlQuery is nil", nil
	}
	res := make([]WrapOffer, 0, 200)
	rows, err := s.db.GetRows(sqlQuery)
	if err != nil {
		res := "[Warn] queryDb query failed: " + err.Error() + " sqlQuery: " + sqlQuery
		return res, nil
	}
	defer rows.Close()

	for rows.Next() {
		var insertDate string
		var offerStr string
		var offer fs.Offer
		if err := rows.Scan(&insertDate, &offerStr); err != nil {
			res := "[Warn] queryDb Scan rows err: " + err.Error() + " sqlQuery: " + sqlQuery
			return res, nil
		}
		err := json.Unmarshal([]byte(offerStr), &offer)
		if err != nil {
			res := "[Warn] queryDb Unmarshal offer err: " + err.Error() + " row: " + offerStr
			return res, nil
		}
		if sketch {
			var wrapOffer = WrapOffer{
				InsertTime: insertDate,
			}
			res = append(res, wrapOffer)
		} else {
			var wrapOffer = WrapOffer{
				InsertTime: insertDate,
				Offer:      &offer,
			}
			res = append(res, wrapOffer)
		}
	}
	return "", res
}

func (s *Service) getNearTable(time string, tables []string) []string {
	var resTables []string

	timeInt, _ := strconv.Atoi(time)
	sort.Strings(tables)
	for i := 0; i < len(tables); i++ {
		date := strings.Split(tables[i], "_")
		if len(date) != 3 {
			s.l.Println("the tableName wrong, name: ", tables[i])
			continue
		}
		dateInt, _ := strconv.Atoi(date[2])
		if timeInt-dateInt >= 0 {
			resTables = append(resTables, tables[i])
		}
	}
	return resTables
}

func HandlerCount(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-type", "application/json; charset=utf-8")
	r.ParseForm()
	dimension := r.Form.Get("dimension")
	condition := r.Form.Get("condition")
	date := r.Form.Get("date")
	num, err := count.CountWithDimension(dimension, condition, date)
	var wrapRes WrapCount
	if err != nil {
		log.Println("HandlerCount err: ", err)
		wrapRes.Num = num
		wrapRes.ErrMsg = err.Error()
	} else {
		wrapRes.Num = num
		wrapRes.ErrMsg = "ok"
	}
	resByte, _ := json.Marshal(&wrapRes)
	w.Write(resByte)
	return

}

func (s *Service) StartServer() {
	http.HandleFunc(s.conf.SearchPath, s.HandlerSearch)

	http.HandleFunc("/count", HandlerCount)

	panic(http.ListenAndServe(s.conf.Host+":"+s.conf.Port, nil))
}
