package task

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/cocopc/gcommons/log"
	"syscall"

	"github.com/cocopc/clickhouse_gsinker/input"
	"github.com/cocopc/clickhouse_gsinker/model"
	"github.com/cocopc/clickhouse_gsinker/output"
	"github.com/cocopc/clickhouse_gsinker/parser"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// 任务结构
type TaskService struct {
	stopped    chan struct{}
	kafka      *input.Kafka
	clickhouse *output.ClickHouse
	p          parser.Parser
	sync.Mutex

	FlushInterval int
	BufferSize    int
}

func NewTaskService(kafka *input.Kafka, clickhouse *output.ClickHouse, p parser.Parser) *TaskService {
	return &TaskService{
		stopped:    make(chan struct{}),
		kafka:      kafka,
		clickhouse: clickhouse,
		p:          p,
	}
}

func (service *TaskService) Init() error{

	service.kafka.Init()

	return service.clickhouse.Init()
}

// 运行任务，如果数据在切片中还没有flush到clickhouse时，程序中断，会丢失掉切片中的数据，因为offset已经被提交，再次启动消费，会从offset消费
func (service *TaskService) Run() {

	err := service.kafka.Start()
	// 如果kafka启动消费失败,直接退出任务
	if err!=nil{
		service.stopped <- struct{}{}
		return
	}
	l.Logf("TaskService %s TaskService has started", service.clickhouse.GetName())
	// 定时器
	tick := time.NewTicker(time.Duration(service.FlushInterval) * time.Second)
	// 预先定义一个100000容量的切片，存储消费到的消息
	msgs := make([]model.Metric, 0, 100000)

	var markMsg *sarama.ConsumerMessage


FOR:
	for {
		select {
		case msg, more := <-service.kafka.Msgs():
			if !more {
				break FOR
			}
			metric,err := service.parse(msg.Value)
			markMsg=msg
			if err != nil{
				l.Errorf("error: %s; msg: %s" ,err.Error(),msg.Value)
				service.kafka.Consumer.MarkOffset(markMsg,"")
				//service.kafka.Consumer.MarkOffset(msgs)
			}else {
				msgs = append(msgs, metric)
			}
			// 如果切片消息数大于定义的buffersize，执行写入clickhouse的操作
			if len(msgs) >= service.BufferSize {
				l.Debug("task run go id: %v",GoID())
				service.Lock()
				err := service.flush(msgs)
				if err != nil {
					syscall.Kill(syscall.Getpid(), syscall.SIGKILL)
				}
				service.kafka.Consumer.MarkOffset(markMsg,"")
				// 刷入clickhouse数据后，重置切片
				msgs = make([]model.Metric, 0, 100000)
				tick = time.NewTicker(time.Duration(service.FlushInterval) * time.Second)
				service.Unlock()
			}
			// 消息没有达到buffersize大小，根据定时器，执行写入clickhouse操作
		case <-tick.C:
			//name:=service.clickhouse.GetName()

			l.Debug("task run go id: %v",GoID())
			l.Logf( " %s tick",service.clickhouse.GetName())
			if len(msgs) == 0 {
				continue
			}
			service.Lock()
			//service.flush(msgs)
			err := service.flush(msgs)
			if err != nil {
				syscall.Kill(syscall.Getpid(), syscall.SIGKILL)
			}
			service.kafka.Consumer.MarkOffset(markMsg,"")

			// 刷入clickhouse数据后，重置切片
			msgs = make([]model.Metric, 0, 100000)
			service.Unlock()
		}
	}
	errs := service.flush(msgs)
	if errs != nil {
		syscall.Kill(syscall.Getpid(), syscall.SIGKILL)
		}
	service.kafka.Consumer.MarkOffset(markMsg,"")
	service.stopped <- struct{}{}
	return
}

// 反序列化消息对象
func (service *TaskService) parse(data []byte) (metric model.Metric ,err error){
	return service.p.Parse(data)
}

// 刷入数据到clickhouse中
func (service *TaskService) flush(metrics []model.Metric) (err error){
	l.Log("buf size:", len(metrics))
	return service.clickhouse.LoopWrite(metrics)
}

// 停止任务释放资源
func (service *TaskService) Stop() {
	l.Log("close TaskService" )
	service.kafka.Stop()
	<-service.stopped
	service.clickhouse.Close()
	l.Log("closed TaskService")
}

//获取goroutine的id
func GoID() int {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	id, err := strconv.Atoi(idField)
	if err != nil {
		panic(fmt.Sprintf("cannot get goroutine id: %v", err))
	}
	return id
}
