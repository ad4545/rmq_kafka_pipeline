package iface

import "rmqkafka_pipeline/pubsub/config"

type Builder interface {
	BuildPipeline(config.RRPipelineConfig) (Pipeline, error)
}
