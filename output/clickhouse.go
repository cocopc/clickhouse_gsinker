package output

import (
	"fmt"
	"github.com/cocopc/clickhouse_gsinker/model"
	"github.com/cocopc/clickhouse_gsinker/pool"
	"github.com/cocopc/clickhouse_gsinker/util"
	"github.com/cocopc/gcommons/log"
	"github.com/cocopc/gcommons/utils"
	"strings"
	"time"
)

type ClickHouse struct {

	Name      string
	TableName string
	Db        string
	Host      string
	Port      int


	Clickhouse  string
	Username    string
	Password    string
	DsnParams   string
	MaxLifeTime time.Duration

	// 定义表中所有的列的切片
	Dims    []*model.ColumnWithType

	// 将表的列名称作为map的key，当前列的全部信息(JsonField,Name,Type)作为值
	dmMap map[string]*model.ColumnWithType

	// 插入的prepare sql
	prepareSQL string

	// 表中的所有列字符串切片
	dms   []string

}


// NewClickHouse new a clickhouse instance
func NewClickHouse() *ClickHouse {
	return &ClickHouse{}
}

// Init the clickhouse intance
func (c *ClickHouse) Init() error {
	return c.initAll()
}

func (c *ClickHouse) initAll() error {
	c.initConn()
	//根据 dms 生成prepare的sql语句
	c.dmMap = make(map[string]*model.ColumnWithType)

	c.dms = make([]string, 0, len(c.Dims))

	for i, d := range c.Dims {
		c.dmMap[d.Name] = c.Dims[i]
		c.dms = append(c.dms, d.Name)
	}

	var params = make([]string, len(c.dmMap))
	for i := range params {
		params[i] = "?"
	}
	c.prepareSQL = "INSERT INTO " + c.Db + "." + c.TableName + " (" + strings.Join(c.dms, ",") + ") VALUES (" + strings.Join(params, ",") + ")"

	l.Logf("Prepare sql %s", c.prepareSQL)
	return nil
}


func (c *ClickHouse) initConn() (err error) {
	var hosts []string

	// if contains ',', that means it's a ip list
	if strings.Contains(c.Host, ",") {
		hosts = strings.Split(strings.TrimSpace(c.Host), ",")
	} else {
		ips, err :=utils.GetIp4Byname(c.Host)
		if err != nil {
			return err
		}
		for _, ip := range ips {
			hosts = append(hosts, fmt.Sprintf("%s:%d", ip, c.Port))
		}
	}


	var dsn = fmt.Sprintf("tcp://%s?username=%s&password=%s", hosts[0], c.Username, c.Password)
	if len(hosts) > 1 {
		otherHosts := hosts[1:]
		dsn += "&alt_hosts="
		dsn += strings.Join(otherHosts, ",")
		dsn += "&connection_open_strategy=random"
	}

	if c.DsnParams != "" {
		dsn += "&" + c.DsnParams
	}
	// dsn += "&debug=1"
	for i := 0; i < len(hosts); i++ {
		pool.SetDsn(c.Host, dsn)
	}
	return
}

// Write kvs to clickhouse
func (c *ClickHouse) Write(metrics []model.Metric) (err error) {
	if len(metrics) == 0 {
		return
	}
	conn := pool.GetConn(c.Host)
	tx, err := conn.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(c.prepareSQL)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, metric := range metrics {
		var args = make([]interface{}, len(c.dmMap))
		for i, name := range c.dms {
			// 获取每个占位符的值，metric就是消费到的JSON数据，c.dmMap[name]标识获取到当前列的结构对象包含(JsonField,Name,Type)
			args[i] = util.GetValueByType(metric, c.dmMap[name])
		}
		if _, err := stmt.Exec(args...); err != nil {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return
	}
	return
}

// LoopWrite will dead loop to write the records
func (c *ClickHouse) LoopWrite(metrics []model.Metric) {
	var count int
	FOR:
	for err := c.Write(metrics); err != nil; {
		count++
		l.Errorf("saving msg error %v %s %s", metrics, err.Error()," will loop to write the data")
		time.Sleep(3 * time.Second)
		if count>=5{
			l.Errorf("Retry 3 times write to ck fail. Msg: %v",metrics)
			break FOR
		}
	}
}


func (c *ClickHouse) Close() error {
	conn:=pool.GetConn(c.Host)
	if conn!=nil{
		conn.Close()
	}
	l.Log("close clickhouse")
	return nil
}

func (c *ClickHouse) GetName() string {
	return c.Name
}

func (c *ClickHouse) Description() string {
	return "clickhouse desc"
}
