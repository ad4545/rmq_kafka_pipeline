package kafkaproducer

import (
	"fmt"
	"rmqkafka_pipeline/pubsub/config"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KPublisher[P any] struct {
	name     string
	config   config.KafkaPublisherConfig
	producer *kafka.Producer
	ready    bool
}

func (kp *KPublisher[P]) Send(msg []byte) error {
	if kp.config.Topic == "" {
		return fmt.Errorf("topic is not set for the producer")
	}

	err := kp.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &kp.config.Topic, Partition: kafka.PartitionAny},
		Value:          msg,
	}, nil)

	if err != nil {
		return fmt.Errorf("failed to send message: %v", err)
	}

	return nil
}

func (r *KPublisher[P]) Run(in <-chan []byte, done chan int, errCh chan error, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-done:
			return
		case msg, ok := <-in:
			if !ok {
				continue
			}
			err := r.Send(msg)
			if err != nil {
				errCh <- fmt.Errorf("publisher %s Send failed: %v", r.name, err)
			}
		}
	}
}

func NewKafkaPublisher[P any](conf config.KafkaPublisherConfig, producer *kafka.Producer) (*KPublisher[P], error) {
	p := &KPublisher[P]{
		config:   conf,
		producer: producer,
	}
	p.ready = true
	p.name = conf.Name

	return p, nil
}
