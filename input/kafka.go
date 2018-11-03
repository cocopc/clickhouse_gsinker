package input

import (
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/cocopc/clickhouse_gsinker/raven"
	"github.com/cocopc/gcommons/log"
	"strings"
	"time"
)

type Kafka struct {
	Name string
	Brokers string
	ConsumerGroup string
	Topic string

	consumer *cluster.Consumer
	stopped  chan struct{}
	msgs     chan []byte

	Sasl struct {
		Username string
		Password string
	}
}


func NewKafka() *Kafka{
	return &Kafka{}
}

// 初始化kafka，定义channel存放消费的消息
func (k *Kafka) Init(){
	k.msgs = make(chan []byte, 300000)
	k.stopped = make(chan struct{})
}

func (k *Kafka) Msgs() chan []byte {
	return k.msgs
}


// KafkaMetaData 获取MeteData
func KafkaMetaData(brokerList string) (sarama.Client,error) {

	config := sarama.NewConfig()
	config.Net.DialTimeout=time.Second * 5
	c, err := sarama.NewClient(strings.Split(brokerList, ","), config)
	if err != nil {
		l.Errorf("Failed get client: %v", err)
		return nil,err
	}
	return c,nil
}

// 开始消费数据
func (k *Kafka) Start() error {
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	if k.Sasl.Username != "" {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = k.Sasl.Username
		config.Net.SASL.Password = k.Sasl.Password
	}

	// 通过metada检查kafka集群是否可以连通，如果不可以连通直接退出，不进行消费逻辑
	_, err:=KafkaMetaData(k.Brokers)
	if err!=nil{
		close(k.stopped)
		return err
	}

	c, err := cluster.NewConsumer(strings.Split(k.Brokers, ","), k.ConsumerGroup, []string{k.Topic}, config)
	l.Debug("创建kafka consumer")
	if err != nil {
		raven.RavenClient.CaptureError(err, nil, nil)
		return err
	}

	k.consumer = c

	//consume errors
	go func() {
		for err := range k.consumer.Errors() {
			l.Errorf("Error: %s", err.Error())
			raven.RavenClient.CaptureError(err, nil, nil)
		}
	}()

	// consume notifications
	go func() {
		for ntf := range k.consumer.Notifications() {
			l.Warnf("Rebalanced: %v", ntf)
			raven.RavenClient.CaptureError(err, nil, nil)
		}
	}()


	go func() {
		l.Logf("Start kafka services %s", k.Name)
	// 定义跳出标识，消费消息错误是跳出
	FOR:
		for {
			select {
				case msg, more := <-k.consumer.Messages():

					if !more {
						l.Errorf("consume kafka message not ok")
						break FOR
					}
					l.Logf("msg: %s",msg.Value)
					k.msgs <- msg.Value
					k.consumer.MarkOffset(msg, "") // mark message as processed

				}
		}
		close(k.stopped)
	}()

	return nil

}


func (k *Kafka) Stop() error {
	l.Log("stop kafka consumer")
	if k.consumer!=nil{
		err:=k.consumer.Close()
		if err!=nil{
			l.Errorf("stop close kafka consumer ",err)
		}
	}
	<-k.stopped
	close(k.msgs)
	return nil
}

func (k *Kafka) Description() string {
	return "kafka consumer:" + k.Topic
}

func (k *Kafka) GetName() string {
	return k.Name
}