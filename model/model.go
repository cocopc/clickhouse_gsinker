package model


// 定义为从kafka消费的JSON消息
type Metric interface {
	Get(key string) interface{}
	GetString(key string) string
	GetFloat(key string) float64
	GetInt(key string) int64
	String() string
}

// 定义clickhouse表的字段及类型，以及从JSON中的取值key。 eg：clichouse字段logday ，从json ({"content":{"logday":"2018-01-01"}})中的content.logday获取
type ColumnWithType struct {
	JsonField string
	Name string
	Type string
}
