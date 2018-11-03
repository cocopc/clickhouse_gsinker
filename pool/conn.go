package pool

import (
	"database/sql"
	"github.com/cocopc/gcommons/log"
	"github.com/kshvakov/clickhouse"
	"sync"
)

type Connection struct {
	*sql.DB
	Dsn string
}

var (
	connNum = 1
	lock    sync.Mutex
)

func (c *Connection) ReConnect() error{

	connect, err := sql.Open("clickhouse", c.Dsn)
	if err != nil {
		return err
	}
	if err := connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			l.Errorf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			l.Error(err)
		}
		return err
	}
	c.DB = connect
	return nil
}

// 用来存放多个clichouse集群的连接,例如ck1集群有【host1 host2 host3 】 三台机器，ck2 集群有【host4 host5 host6】
// 次map用来存放多个集群得连接，map的key为配置中的集群名称。 真正每个集群内hosts的轮训是【https://github.com/kshvakov/clickhouse.git】
// 实现的，通过连接参数上指定 alt_hosts=host2:9000,host3:9000 实现单个集群内的每个host的轮训。由于本项目支持灵活多任务，可以指定数据发到
// 不同的clickhouse集群，所有需要map来管理起来各个集群的连接
var poolMaps = map[string][]*Connection{}


func SetDsn(name string, dsn string) {
	lock.Lock()
	defer lock.Unlock()

	connect, err := sql.Open("clickhouse", dsn)
	if err != nil {
		panic(err)
	}

	if err := connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			l.Errorf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			l.Error(err)
		}
		return
	}

	if ps, ok := poolMaps[name]; ok {
		//当前的connNum 就是1，对应一个clichouse dsn只需要创建一个连接。该dsn的多台机器通过alt_hosts参数，
		// 由【https://github.com/kshvakov/clickhouse.git】实现轮训
		l.Debugf("clickhouse dsn %s %s", dsn,len(ps))
		if len(ps) >= connNum {
			l.Logf("arrive max cluster conn,current conn %s,max conn %s", ps,connNum)
			return
		}
		ps = append(ps, &Connection{connect, dsn})
	} else {
		l.Debugf("clickhouse dsn %s %s", dsn,len(ps))
		poolMaps[name] = []*Connection{&Connection{connect, dsn}}
	}
}




func GetConn(name string) *Connection {
	lock.Lock()
	defer lock.Unlock()
	ps := poolMaps[name]
	//return ps[rand.Intn(len(ps))]
	// 只会有一个连接
	if len(ps)>0{
		return  ps[0]
	}else{
		return nil
	}
}

func CloseAll() {
	for _, ps := range poolMaps {
		for _, c := range ps {
			c.Close()
		}
	}
}

