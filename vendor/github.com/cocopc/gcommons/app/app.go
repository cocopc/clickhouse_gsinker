package app

import (
	"github.com/cocopc/gcommons/log"
	"github.com/cocopc/gcommons/utils"
	"os"
)

var (
	log *l.Logger
)

func init(){

	log = l.New()
	log.Prefix=log.Colorize("[gcommons]",l.Green)

}

// Run 运行一个应用，需要初始化函数，运行作业函数，清理资源函数，收到退出信号时清理资源
func Run(appName string,initFunc,jobFunc,cleanupFunc func() error)  {
	log.Printf("Initial [%s]", appName)
	if err := initFunc(); err != nil {
		log.Printf("Initial [%s] failure: [%s]", appName, err)
		panic(err)
	}
	log.Printf("Initial [%s] complete", appName)
	go func() {
		if err := jobFunc(); err != nil {
			log.Printf("[%s] run error: [%v]", appName, err)
			panic(err)
		}
	}()

	utils.WaitExitSign()
	log.Printf("[%s] watched the exit signal, start to clean", appName)
	if err := cleanupFunc(); err != nil {
		log.Printf("[%s] clean failed: [%v]", appName, err)
		panic(err)
	}
	log.Printf("[%s] clean complete, exited", appName)

	os.Exit(0)

}