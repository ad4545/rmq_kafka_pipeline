package channel

import (
	"fmt"
	"rmqkafka_pipeline/pubsub/config"
	"rmqkafka_pipeline/pubsub/publishers"
	"rmqkafka_pipeline/pubsub/publishers/kafkaproducer"
	"rmqkafka_pipeline/pubsub/subscribers"
	"rmqkafka_pipeline/pubsub/subscribers/rmq"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type pipeline[S any, P any] struct {
	subscriber   subscribers.Subscriber[S]
	publisher    publishers.Publisher[P]
	bridge       *Bridge[S, P]
	config_rmq   config.RMQConfig
	in           chan []byte
	out          chan []byte
	errorChannel chan error
	done         chan int
	name         string
	active       bool
}

func NewPipeline[S any, P any](conf config.RRPipelineConfig) (*pipeline[S, P], error) {
	in := make(chan []byte)
	out := make(chan []byte)
	errCh := make(chan error)
	done := make(chan int)

	// connecting to rabbitMQ Server
	conn, err := rmq.NewRabbitMQ(conf.RMQConnConfig)

	if err != nil {
		return nil, fmt.Errorf("failed to connect RabbitMq server %s:%v", conf.Name, err)
	}

	// creating Kafka producer
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": conf.KafkaConnConfig.Broker,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka Producer %s:%v", conf.Name, err)
	}
	// open a rabbitMq channel
	client, err := conn.NewClient(conf.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to create RMQ Client %s: %v", conf.Name, err)
	}
	//  creating subscriber for RabbitMq
	sub, err := rmq.NewRabbitMQSubscriber[S](conf.RMQSubConfig, client)

	if err != nil {
		return nil, fmt.Errorf("failed to create RMQ Subscriber %s: %v", conf.Name, err)
	}

	pub, err := kafkaproducer.NewKafkaPublisher[P](conf.KafkaConfig, producer)

	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka Producer %s: %v", conf.Name, err)
	}

	b := NewBridge[S, P](conf.Name, in, out, done, errCh)
	// b.SetConverter(msgConverter)
	return &pipeline[S, P]{
		subscriber:   sub,
		publisher:    pub,
		bridge:       b,
		config_rmq:   conf.RMQConnConfig,
		in:           in,
		out:          out,
		errorChannel: errCh,
		done:         done,
		name:         conf.Name,
	}, nil
}

func (p *pipeline[S, P]) IsActive() bool {
	return p.active
}

func (p *pipeline[S, P]) Deactivate() {
	p.active = false
}

func (p *pipeline[S, P]) Shutdown() {}

func (p *pipeline[S, P]) Start(wg *sync.WaitGroup) {

	p.active = true

	defer wg.Done()
	fmt.Printf("Started pipeline %s\n", p.name)
	wg.Add(1)
	go p.bridge.Run(wg)
	err := p.subscriber.Initialise(p.in, p.config_rmq)
	if err != nil {
		p.errorChannel <- err
		return
	}
	wg.Add(1)
	go p.publisher.Run(p.out, p.done, p.errorChannel, wg)
}

func (p *pipeline[S, P]) GetErrorStream() chan error {
	return p.errorChannel
}

func (p *pipeline[S, P]) Name() string {
	return p.name
}
