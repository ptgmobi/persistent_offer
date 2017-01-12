package fetch_snapshot

import (
	"encoding/json"
	"net/http"
	"fmt"
	"reflect"
	"errors"

	"github.com/dongjiahong/gotools"

	dbCore "db_core"
)

type Conf struct {
	FetchApi		string `json:"fetch_api"`
	FetchFrequency  int `json:"fetch_frequency"` // fetch 频率（分钟）

	LogPath			string `json:"log_path"`
}

type Service struct {
	conf		*Conf
	l			*gotools.RotateLog
}

//type Snapshot struct {
	//Data		 []Offer	`json:"data"`
	//TotalRecords int		`json:"total_records"`
//}

type Offer struct {
	Active	bool   `json:"active"`
	Dnf		string `json:"dnf"`
	Docid	string `json:"docid"`
	Name	string `json:"name"`
	Attr	Attribute `json:"attr"`
}

type Attribute struct {
	AdExpireTime		int `json:"ad_expire_time"`
	Adid				string `json:"adid"`
	AppCategory			[]string `json:"app_category"`
	AppDown				AppDownload `json:"app_download"`
	Channel			string `json:"channel"`
	ClickCallback	string `json:"click_callback"`
	ClkTks			[]string `json:"clk_tks"`
	ClkUrl			string `json:"clk_url"`
	Countries		[]string `json:"countries"`
	Creatives		CreativeLanguage `json:"creatives"`
	FinalUrl		string `json:"final_url"`
	Icons			CreativeLanguage `json:"icons"`
	LandingType		int `json:"landing_type"`
	Payout			float32 `json:"payout"`
	Platform		string `json:"platform"`
	ProductCategory string `json:"product_category"`
	RenderImgs		RenderImg `json:"render_imgs"`
}

type AppDownload struct {
	AppPkgName		string `json:"app_pkg_name"`
	Description		string `json:"description"`
	Download		string `json:"download"`
	Rate			float32 `json:"rate"`
	Review			int     `json:"review"`
	Size			string `json:"size"`
	Title			string `json:"title"`
	TrackingLink	string `json:"tracking_link"`
}

type CreativeLanguage struct {
	ALL		[]Creative `json:"ALL"`
}

type Creative struct {
	Height		int	`json:"height"`
	Language	string `json:"language"`
	Url			string `json:"url"`
	Width		int `json:"width"`
}

type RenderImg struct {
	R500500		string `json:"500500"`
	R7201280	string `json:"7201280"`
	R950500		string `json:"950500"`
}

func NewService(conf *Conf) (*Service, err) {
	l, err := gotools.NewRotateLog(conf.LogPath, "", log.LUTC|log.LstdFlags)
	if err != nil {
		fmt.Println("[FetchSnapshot] create log err: ", err)
	}

	srv := &Service {
		conf:	conf,
		l:		l,
	}

	return srv, nil
}

func FetchWithFrequency() {
}

func (s *Service) fetchSnapshot() error {
	if len(s.conf.FetchApi) == 0 {
		s.l.Println(" >>>>> FetchApi is nil")
		return nil
	}
	// https://api.cloudmobi.net:9992/dump?channel=any&country=DEBUG&platform=

	resp, err := http.Get(s.conf.FetchApi)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return err
	}

	snapShotOfferCnt := 0
	dec := json.NewDecoder(resp.Body)

	for {
		t, err := dec.Token()
		if err != nil {
			s.l.Println("dec Token err: ", err)
			break
		}

		if _, ok := t.(json.Delim); ok {
			if dec.More() {
				continue
			}
			break
		}

		if key, ok := t.(string); !ok {
			return errors.New("unexpected code type: " + reflect.TypeOf(key).Name())
		}
		switch key {
		case "total_records":
			t, err = dec.Token()
			if err != nil {
				s.l.Println("unexpected error when parsing status: ", err)
				return err
			}
			if v, ok := t.(float64); ok {
				if v <= int(0) {
					return errors.New("call api err, total_records: " + fmt.Sprintf("%f", v))
				}
			} else {
				return errors.New("total_records type error: " + reflect.TypeOf(t).Name())
			}
		case "data":
			t, err = dec.Token() // read [
			if err != nil {
				return errors.New("read open bracket: " + err.Error())
			}
			for dec.More() {
				var item Offer
				if err := dec.Decode(&item); err != nil {
					s.l.Println("[JSON] decode item error: ", err)
					continue
				}
				// TODO 加入数据库
			}

			t, err = dec.Token() // read ]
			if err != nil {
				s.l.Println("unexpected error when reading data close bracket: ", err)
				return err
			}

		default:
			s.l.Println("un-handled key of [", key , "]")
		}

	}

	if  snapShotOfferCnt <= 0 {
		return errors.New(fmt.Sprintf("snapShotOfferCnt: %d", snapShotOfferCnt))
	}

	s.l.Println("fetchSnapshot ok!")
	return nil
}
