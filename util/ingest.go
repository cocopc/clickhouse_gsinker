package util

import (
	"encoding/json"
)

func IngestConfig(config interface{}, entity interface{}) {
	bs, _ := json.Marshal(config)
	json.Unmarshal(bs, entity)
}
