package kafkaproducer

import (
	"fmt"
	"rmqkafka_pipeline/pubsub/config"
	"sync"

	"github.com/IBM/sarama"
)

type KPublisher[P any] struct {
	name     string
	config   config.KafkaPublisherConfig
	producer sarama.SyncProducer
	ready    bool
}

func (kp *KPublisher[P]) Send(msg []byte) error {
	if kp.config.Topic == "" {
		return fmt.Errorf("topic is not set for the producer")
	}
    message := &sarama.ProducerMessage{
		Topic: kp.config.Topic,
		Value: sarama.ByteEncoder(msg),
	}
	

	partition, offset,err := kp.producer.SendMessage(message)

	if err != nil {
        fmt.Println("Error producing message:", err)
		fmt.Println("Partition producing message:", partition)
		fmt.Println("Offset producing message:", offset)
        return err
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

func NewKafkaPublisher[P any](conf config.KafkaPublisherConfig, producer sarama.SyncProducer) (*KPublisher[P], error) {
	p := &KPublisher[P]{
		config:   conf,
		producer: producer,
	}
	p.ready = true
	p.name = conf.Name

	return p, nil
}
