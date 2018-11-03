package main

import (
	conf "github.com/cocopc/clickhouse_gsinker/config"
	"github.com/cocopc/clickhouse_gsinker/raven"
	"github.com/cocopc/clickhouse_gsinker/task"
	"github.com/cocopc/gcommons/app"
	"net/http"
	_ "net/http/pprof"
)


func main() {

	var skr *Sinker

	app.Run("clickhouse_gsinker", func() error {
		// 初始化配置
		cfg := *conf.InitConfig()
		raven.InitRavenClient(cfg.Raven.Dsn,cfg.Raven.Username,cfg.Raven.Email)
		// pprof 监控地址
		go func() {
			http.ListenAndServe(cfg.Common.Pprof, nil)
		}()
		skr = NewSinker(cfg)
		// 生成任务配置
		return skr.Init()

	}, func() error {

		skr.Run()
		return nil

	}, func() error {

		skr.Close()
		return nil

	})

}


// 定义发送者
type Sinker struct {
	tasks   []*task.TaskService
	config  conf.Config
	stopped chan struct{}
}

func NewSinker(config conf.Config) *Sinker {
	s := &Sinker{config: config, stopped: make(chan struct{})}
	return s
}

func (s *Sinker) Init() error {
	s.tasks = s.config.GenTasks()
	for _, t := range s.tasks {
		t.Init()
	}
	return nil
}

// 运行任务
func (s *Sinker) Run() {
	for i := range s.tasks {
		go s.tasks[i].Run()
	}
	<-s.stopped
}

// 关闭任务释放资源
func (s *Sinker) Close() {
	for i := range s.tasks {
		s.tasks[i].Stop()
	}
	close(s.stopped)
}
