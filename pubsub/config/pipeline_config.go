package config

import (
	"context"
)

type KafkaConfig struct {
	Broker1 string `yaml:"broker1"`
	Broker2 string `yaml:"broker2"`
}

type KafkaPublisherConfig struct {
	Topic string
	Name  string // name; will be same as channel name
}

type RMQConfig struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Host     string `yaml:"host"`
	Vhost    string `yaml:"vhost"`
}

type RMQClientConfig struct {
	Exchange   string
	Topic      string
	RoutingKey string
	Durable    bool
	Autodelete bool
	Ctx        context.Context
}

// Ros Rmq Pipeline Config
type RRPipelineConfig struct {
	KafkaConfig     KafkaPublisherConfig
	RMQConnConfig   RMQConfig
	KafkaConnConfig KafkaConfig
	RMQSubConfig    RMQClientConfig
	Name            string //name of the channel
}
