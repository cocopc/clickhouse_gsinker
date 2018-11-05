package parser

import (
	"fmt"
	"github.com/cocopc/clickhouse_gsinker/model"
	"github.com/cocopc/gcommons/log"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"time"
	"github.com/cocopc/gcommons/utils"
)

type ECPParser struct {

}

func init()  {
	RegParser["ecp"]=&ECPParser{}
}


func (c *ECPParser) Parse(bs []byte) model.Metric {
	mstr := utils.Bytes2Str(bs)

	if gjson.Valid(mstr) {
		ctime:=gjson.Get(mstr, "ctime").Int()

		t := time.Unix(0,ctime * 1000000)
		logday := t.Format("2006-01-02")
		hour := t.Hour()
		minute := t.Minute()
		// 根据ctime出去日期，小时，和分钟，放到udate字段中
		sstr,err := sjson.Set(mstr,"udate",map[string]interface{}{"logday":logday,"hour":hour,"minute":minute})

		if err!=nil{
			l.Errorf("ECP set json fail %v",err.Error())
			return nil
		}

		return &ECPMetric{sstr}
	}else {
		return nil
	}

}


type ECPMetric struct {
	raw string
}

func (c *ECPMetric) String() string {
	return fmt.Sprintf(c.raw)
}

func (c *ECPMetric) Get(key string) interface{} {
	return gjson.Get(c.raw, key).Value()
}

func (c *ECPMetric) GetString(key string) string {

	return gjson.Get(c.raw, key).String()
}

func (c *ECPMetric) GetFloat(key string) float64 {
	return gjson.Get(c.raw, key).Float()
}

func (c *ECPMetric) GetInt(key string) int64 {
	return gjson.Get(c.raw, key).Int()
}




