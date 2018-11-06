package parser

import (
	"fmt"
	"github.com/cocopc/clickhouse_gsinker/model"
	"github.com/pkg/errors"
	"github.com/tidwall/gjson"
)


type GjsonParser struct {

}

func init()  {

	RegParser["gjson"]=&GjsonParser{}
}


//func NewParser() Parser {
//
//	return &GjsonParser{}
//
//}

func (c *GjsonParser) Parse(bs []byte) (metric model.Metric ,err error){
	mstr := string(bs)
	if gjson.Valid(mstr) {
		return &GjsonMetric{mstr},nil
	}else {
		return nil,errors.New("不是合法的JSON")
	}

}



type GjsonMetric struct {
	raw string
}

func (c *GjsonMetric) String() string {
	return fmt.Sprintf(c.raw)
}

func (c *GjsonMetric) Get(key string) interface{} {
	return gjson.Get(c.raw, key).Value()
}

func (c *GjsonMetric) GetString(key string) string {

	return gjson.Get(c.raw, key).String()
}

func (c *GjsonMetric) GetFloat(key string) float64 {
	return gjson.Get(c.raw, key).Float()
}

func (c *GjsonMetric) GetInt(key string) int64 {
	return gjson.Get(c.raw, key).Int()
}
