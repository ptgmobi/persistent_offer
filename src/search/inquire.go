// inquire 提供简单的offer快照查询功能
package search

import(
	"net/http"

	"github.com/dongjiahong/gotools"

	dbCore "db_core"
)

type Conf struct {
	SearchPath     string `json:"search_path"`
	Port		   string `json:"port"`
	Host		   string `json:"host"`
}

type Service struct {
	conf *Conf
	l    *gotools.RotateLog
	db   *dbCore.DBCore
}

func NewService(conf *Conf, dbConf *dbCore.Conf) (*Service) {
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

func (s *Service) HandlerSearch() {
}

func (s *Service) StartServer() {
	http.HandleFunc(s.conf.SearchPath, s.HandlerSearch)

	panic(http.ListenAndServe(s.conf.Host+":"+s.conf.Port, nil)
}
