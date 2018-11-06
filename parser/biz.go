package parser

import (
	"fmt"
	"github.com/cocopc/clickhouse_gsinker/model"
	"github.com/cocopc/gcommons/log"
	"github.com/pkg/errors"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"time"
	"github.com/cocopc/gcommons/utils"
)

type ECPParser struct {

}

func init()  {
	RegParser["biz"]=&ECPParser{}
}


func (c *ECPParser) Parse(bs []byte) (metric model.Metric,err error){
	mstr := utils.Bytes2Str(bs)

	if gjson.Valid(mstr) {
		ctime:=gjson.Get(mstr, "ctime").Int()
		if ctime==0{
			return nil,errors.New("无ctime字段")
		}
		t := time.Unix(0,ctime * 1000000)
		logday := t.Format("2006-01-02")
		fullTime := t.Format("2006-01-02 15:04:05")
		hour := t.Hour()
		minute := t.Minute()
		// 根据ctime出去日期，小时，和分钟，放到udate字段中
		sstr,err := sjson.Set(mstr,"udate",map[string]interface{}{"logday":logday,"full_time":fullTime,"hour":hour,"minute":minute})

		if err!=nil{
			l.Errorf("biz set json fail %v",err.Error())
			return nil,errors.New("生成日期字段错误")
		}

		return &ECPMetric{sstr},nil
	}else {
		return nil,errors.New("不是合法的JSON")
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




