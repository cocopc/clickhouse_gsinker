package config

import (
	"github.com/cocopc/clickhouse_gsinker/input"
	"github.com/cocopc/clickhouse_gsinker/output"
	"github.com/cocopc/clickhouse_gsinker/task"
	"github.com/cocopc/clickhouse_gsinker/util"
	"github.com/cocopc/clickhouse_gsinker/parser"
)

// GenTasks generate the tasks via config

func (config *Config) GenTasks() []*task.TaskService {
	res := make([]*task.TaskService, 0, len(config.Tasks))
	// 生成多个任务的配置
	for _, taskConfig := range config.Tasks {

		// 生成kafka配置
		kafka := config.GenInput(taskConfig)
		// 生成clickhouse配置
		ck := config.GenOutput(taskConfig)

		// 定义JSON解析器
		p := parser.NewParser()
		taskImpl := task.NewTaskService(kafka, ck, p)

		// 注册相关的属性到对象
		util.IngestConfig(taskConfig, taskImpl)

		// 保证flushinterval buffersize 配置大于0
		if taskImpl.FlushInterval == 0 {
			taskImpl.FlushInterval = config.Common.FlushInterval
		}

		if taskImpl.BufferSize == 0 {
			taskImpl.BufferSize = config.Common.BufferSize
		}

		res = append(res, taskImpl)
	}
	return res
}

// GenInput generate the input via config
func (config *Config) GenInput(taskCfg *Task) *input.Kafka {
	// 从task config中获取到使用kafka集群map key，生成对应的kafka对象
	kfkCfg := config.Kafka[taskCfg.Kafka]
	inputImpl := input.NewKafka()

	util.IngestConfig(taskCfg, inputImpl)
	util.IngestConfig(kfkCfg, inputImpl)
	return inputImpl
}

// GenOutput generate the output via config
func (config *Config) GenOutput(taskCfg *Task) *output.ClickHouse {
	// 从task config中获取到使用clichouse集群map key，生成对应的clichouse对象
	ckCfg := config.Clickhouse[taskCfg.Clickhouse]

	outputImpl := output.NewClickHouse()

	util.IngestConfig(ckCfg, outputImpl)
	util.IngestConfig(taskCfg, outputImpl)
	util.IngestConfig(config.Common, outputImpl)
	return outputImpl
}

