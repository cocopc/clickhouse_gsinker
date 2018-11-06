package parser

import "github.com/cocopc/clickhouse_gsinker/model"

var RegParser=make(map[string]interface{})


type Parser interface {
	Parse(bs []byte) (metric model.Metric,err error)

}

