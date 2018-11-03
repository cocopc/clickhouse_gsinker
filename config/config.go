package config

import (
	"github.com/cocopc/gcommons/log"
	"github.com/k0kubun/pp"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type KafkaConfig struct {
	Brokers string
	Sasl struct{
		Password string
		Username string
	}
}

type ClickhouseConfig struct {
	Db string
	Host string
	Port int
	Username string
	Password string

}

type Task struct {
	Name string
	Kafka string
	Topic string
	ConsumerGroup string
	Clickhouse string
	TableName string

	Dims []struct {
		Name string
		Type string
		JsonField string
	}

	BufferSize int
	FlushInterval int
}


type Config struct {
	Kafka map[string]*KafkaConfig
	Clickhouse map[string]*ClickhouseConfig
	Tasks []*Task

	Common struct{
		FlushInterval int
		BufferSize int
		LogType string
	}
	Raven struct{
		Dsn string
		Username string
		Email string
	}

}

var (
	defaultFlushInterval = 3
	defaultBufferSize    = 20000
	baseConfig *Config
)


// 初始化配置
func InitConfig() *Config{

	pflag.String("confPath","/Users/panchao/workspace/go/src/github.com/cocopc/clickhouse_gsinker/config/example","config path")
	pflag.Parse()
	viper.BindPFlags(pflag.CommandLine)

	viper.SetConfigName("global")
	confPath := viper.GetString("confPath")
	//设置搜索配置文件路径
	viper.AddConfigPath(confPath)
	viper.AddConfigPath(".")


	//读取全局配置文件
	err := viper.ReadInConfig()
	if err != nil { // Handle errors reading the config file
		panic(pp.Errorf("Fatal error config file: %s \n", err))
	}


	// 读取tasks配置文件
	viper.SetConfigName("tasks")
	errs := viper.MergeInConfig()
	if errs != nil { // Handle errors reading the config file
		panic(pp.Errorf("Fatal error config file: %s \n", errs))
	}

	//pp.Println(viper.AllSettings())
	baseConfig = &Config{}
	viper.Unmarshal(&baseConfig)

	if baseConfig.Common.FlushInterval < 1 {
		baseConfig.Common.FlushInterval = defaultFlushInterval
	}

	if baseConfig.Common.BufferSize < 1 {
		baseConfig.Common.BufferSize = defaultBufferSize
	}

	pp.Println(baseConfig)

	l.Define("[clichouse_gsinker]",l.Blue,baseConfig.Common.LogLevel)

	return baseConfig
}








