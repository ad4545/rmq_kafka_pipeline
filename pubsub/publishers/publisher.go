package publishers

import (
	"sync"
)

type Publisher[P any] interface {
	// Configure(*kafka.ConfigMap) error                    // Configures the producer with Kafka settings
	// Produce(P) error                                     // Sends a message to Kafka
	// SetTopic(string) Publisher[P]                        // Sets the Kafka topic
	// SetBrokers(string) Publisher[P]                      // Sets the Kafka brokers
	Run(<-chan []byte, chan int, chan error, *sync.WaitGroup) // Runs the producer to consume messages from a channel
}
