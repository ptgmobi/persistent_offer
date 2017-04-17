package restful

import (
	"encoding/json"
	"log"
	"net/http"
)

type Conf struct {
	DimensionPath string `json:"search_dimension_path"`
}

// channel, platform, country (如果offer包含多个国家，可拆分国家并统计多次）

func Server() {
	http.HandleFunc()
}
