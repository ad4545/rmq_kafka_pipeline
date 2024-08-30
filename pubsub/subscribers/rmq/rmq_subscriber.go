package rmq

import (
	"fmt"
	"rmqkafka_pipeline/pubsub/config"
)

// rabbitMQSubscriber represents a RabbitMQ subscriber with generic data type S.
type rabbitMQSubscriber[S any] struct {
	client      RabbitClient
	conf        config.RMQClientConfig
	outChannel  chan<- []byte
	initialised bool
}

func (r *rabbitMQSubscriber[S]) Configure() error {
	if r.client == nil {
		return fmt.Errorf("client not set")
	}

	if err := r.client.NewExchangeDeclare(r.conf.Exchange, "topic", r.conf.Durable, r.conf.Autodelete); err != nil {
		return err
	}

	if err := r.client.NewQueueDeclare(r.conf.Topic, r.conf.Durable, r.conf.Autodelete); err != nil {
		return err
	}

	if err := r.client.CreateBinding(r.conf.Topic, r.conf.RoutingKey, r.conf.Exchange); err != nil {
		return err
	}

	return nil
}

// NewRabbitMQSubscriber creates a new RabbitMQ subscriber with the given configuration.
func NewRabbitMQSubscriber[S any](conf config.RMQClientConfig, client RabbitClient) (*rabbitMQSubscriber[S], error) {
	sub := &rabbitMQSubscriber[S]{
		conf:   conf,
		client: client,
	}

	err := sub.Configure()

	if err != nil {
		return nil, err
	}

	return sub, nil
}

// Initialise sets up the RabbitMQ connection and starts consuming messages.
func (s *rabbitMQSubscriber[S]) Initialise(out chan<- []byte, rmqConnConfig config.RMQConfig) error {
	if s.initialised {
		return fmt.Errorf("subscriber is already initialised")
	}

	// Initialize RabbitMQ client if not already done
	if s.client == nil {
		if err := s.setupClient(rmqConnConfig); err != nil {
			return err
		}
	}

	// Set output channel if not already set
	if s.outChannel == nil {
		s.outChannel = out
	}

	// Declare queue and optional exchange/bindings
	if err := s.declareQueueAndExchange(); err != nil {
		return err
	}

	// Start consuming messages from RabbitMQ
	if err := s.startConsumingMessages(); err != nil {
		return err
	}

	s.initialised = true
	return nil
}

// setupClient initializes the RabbitMQ client.
func (s *rabbitMQSubscriber[S]) setupClient(rmqConnConfig config.RMQConfig) error {
	rabbitMQ, err := NewRabbitMQ(rmqConnConfig)
	if err != nil {
		return fmt.Errorf("failed to initialize RabbitMQ connection: %v", err)
	}

	client, err := rabbitMQ.NewClient(s.conf.Topic)
	if err != nil {
		return fmt.Errorf("failed to create RabbitMQ client: %v", err)
	}

	s.client = client
	return nil
}

// declareQueueAndExchange declares the queue and optionally sets up an exchange and bindings.
func (s *rabbitMQSubscriber[S]) declareQueueAndExchange() error {
	// Declare the queue
	if err := s.client.NewQueueDeclare(s.conf.Topic, s.conf.Durable, s.conf.Autodelete); err != nil {
		return fmt.Errorf("failed to declare queue %s: %v", s.conf.Topic, err)
	}

	// Declare exchange and bind queue to it, if specified
	if s.conf.Exchange != "" {
		if err := s.client.NewExchangeDeclare(s.conf.Exchange, "topic", s.conf.Durable, s.conf.Autodelete); err != nil {
			return fmt.Errorf("failed to declare exchange %s: %v", s.conf.Exchange, err)
		}

		if err := s.client.CreateBinding(s.conf.Topic, s.conf.RoutingKey, s.conf.Exchange); err != nil {
			return fmt.Errorf("failed to bind queue %s to exchange %s: %v", s.conf.Topic, s.conf.Exchange, err)
		}
	}
	return nil
}

// startConsumingMessages starts a goroutine to consume messages from RabbitMQ and process them.
func (s *rabbitMQSubscriber[S]) startConsumingMessages() error {
	// Start consuming messages
	msgs, err := s.client.Receive(s.conf.Ctx, s.conf.Topic, "", true)
	if err != nil {
		return fmt.Errorf("failed to register a consumer: %v", err)
	}
	go func() {
		for d := range msgs {
			// Call the Callback method with the deserialized data
			s.Callback(&d.Body)
		}
	}()

	return nil
}

// callback processes the received message and sends it to the output channel.
func (s *rabbitMQSubscriber[S]) Callback(msg *[]byte) { // Update signature to match interface
	if !s.initialised {
		return
	}

	// Send the message to the output channel

	if msg != nil {
		s.outChannel <- *msg
	}
}
