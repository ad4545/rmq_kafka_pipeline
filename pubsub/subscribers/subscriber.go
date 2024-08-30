package subscribers

import "rmqkafka_pipeline/pubsub/config"

// Subscriber interface for subscribers in the system
type Subscriber[S any] interface {
	Callback(msg *[]byte)                             // Exported method
	Initialise(chan<- []byte, config.RMQConfig) error // Matching signature with the implementation
}
