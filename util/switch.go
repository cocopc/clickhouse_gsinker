package util

import (
	"github.com/cocopc/clickhouse_gsinker/model"
)

//这里对metric的value类型，只有三种情况， （float64，string，map[string]interface{})
func GetValueByType(metric model.Metric, cwt *model.ColumnWithType) interface{} {
	swType := switchType(cwt.Type)
	switch swType {
	case "int":
		return metric.GetInt(cwt.JsonField)
	case "float":
		return metric.GetFloat(cwt.JsonField)
	case "string":
		return metric.GetString(cwt.JsonField)
		//never happen
	default:
		return ""
	}
}

func switchType(typ string) string {
	switch typ {
	case "UInt8", "UInt16", "UInt32", "UInt64", "Int8", "Int16", "Int32", "Int64":
		return "int"
	case "String", "FixString":
		return "string"
	case "Float32", "Float64":
		return "float"
	default:
		panic("unsupport type " + typ)
	}
}

