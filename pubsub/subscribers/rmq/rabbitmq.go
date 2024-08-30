package rmq

import (
	"context"
	"fmt"
	"rmqkafka_pipeline/pubsub/config"
	"strings"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

var lock = &sync.Mutex{}

type RabbitMQMaster interface {
	Connect() RabbitMQMaster
	HasClient(name string) bool
	NewClient(name string) (RabbitClient, error)
	RemoveClient(name string) error
	Close() error
}

type RabbitClient interface {
	Close() error
	NewExchangeDeclare(exchangeName, kind string, durable, autodelete bool) error
	NewQueueDeclare(queueName string, durable, autodelete bool) error
	CreateBinding(name string, binding string, exchange string) error
	Send(ctx context.Context, exchange, routingKey string, options amqp.Publishing) error
	Receive(ctx context.Context, queue, consumer string, autoAck bool) (<-chan amqp.Delivery, error)
}

type rabbitClient struct {
	// rule of thumb is to use a single connection per app and spawn channels for every task
	conn *amqp.Connection // a tcp connection used by the client
	ch   *amqp.Channel    // a multiplexed connection over the tcp connection i.e, conn
}

var rabbitMQSingleton *rabbitMQ

type rabbitMQ struct {
	conn    *amqp.Connection
	conf    config.RMQConfig
	clients map[string]*rabbitClient
}

func (r *rabbitMQ) Connect() (*rabbitMQ, error) {

	// Remove "http://" if present and change the port to 5672
	host := r.conf.Host
	if strings.HasPrefix(host, "http://") {
		host = strings.TrimPrefix(host, "http://")
	}

	// Split the host string to replace the port
	hostParts := strings.Split(host, ":")
	if len(hostParts) == 2 {
		hostParts[1] = "5672" // Change port to 5672
	}
	host = strings.Join(hostParts, ":")
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/%s",
		r.conf.Username, r.conf.Password, host, r.conf.Vhost))

	if err != nil {
		return nil, err
	}

	r.conn = conn
	return r, nil
}

func createRabbitMQ(conf config.RMQConfig) (*rabbitMQ, error) {

	if conf.Username == "" {
		return nil, fmt.Errorf("Empty username provided")
	}

	if conf.Password == "" {
		return nil, fmt.Errorf("Empty password provided")
	}

	if conf.Host == "" {
		return nil, fmt.Errorf("host cannot be an empty string")
	}

	if conf.Vhost == "" {
		return nil, fmt.Errorf("vhost cannot be an empty string")
	}

	r := &rabbitMQ{
		conf:    conf,
		clients: map[string]*rabbitClient{},
	}

	_, err := r.Connect()

	if err != nil {
		return nil, fmt.Errorf("Failed to connect with %v: %v", r.conf, err)
	}

	return r, nil

}

func NewRabbitMQ(conf config.RMQConfig) (*rabbitMQ, error) {

	if rabbitMQSingleton == nil {
		lock.Lock()
		defer lock.Unlock()
		r, err := createRabbitMQ(conf)
		if err != nil {
			return nil, fmt.Errorf("RMQ Creation error: %v", err)
		}
		rabbitMQSingleton = r
	}
	return rabbitMQSingleton, nil
}

func (r *rabbitMQ) HasClient(name string) bool {
	if _, ok := r.clients[name]; !ok {
		return false
	}
	return true
}

func (r *rabbitMQ) Close() error {
	return r.conn.Close()
}

func (r *rabbitMQ) NewClient(name string) (*rabbitClient, error) {
	if r.conn == nil {
		return nil, fmt.Errorf("Cannot add clients to an uninitialized RMQ Connection")
	}

	if r.HasClient(name) {
		return nil, fmt.Errorf("Client already exists, please provide a new key")
	}

	ch, err := r.conn.Channel()

	if err != nil {
		return nil, err
	}

	r.clients[name] = &rabbitClient{
		conn: r.conn,
		ch:   ch,
	}

	return r.clients[name], nil
}

func (r *rabbitMQ) RemoveClient(name string) error {
	if !r.HasClient(name) {
		return fmt.Errorf("Client %s does not exist", name)
	}

	r.clients[name].Close()
	return nil
}

func (rc *rabbitClient) Close() error {
	return rc.ch.Close()
}

func (rc *rabbitClient) NewExchangeDeclare(exchangeName, kind string, durable, autodelete bool) error {
	err := rc.ch.ExchangeDeclare(exchangeName, kind, durable, autodelete, false, false, nil)
	return err
}

func (rc *rabbitClient) NewQueueDeclare(queueName string, durable, autodelete bool) error {
	_, err := rc.ch.QueueDeclare(queueName, durable, autodelete, false, false, nil)
	return err
}

func (rc *rabbitClient) CreateBinding(name string, binding string, exchange string) error {
	return rc.ch.QueueBind(name, binding, exchange, false, nil)
}

func (rc *rabbitClient) Send(ctx context.Context, exchange, routingKey string, options amqp.Publishing) error {
	return rc.ch.PublishWithContext(ctx, exchange, routingKey, true, false, options)
}

func (rc *rabbitClient) Receive(ctx context.Context, queue, consumer string, autoAck bool) (<-chan amqp.Delivery, error) {
	return rc.ch.Consume(queue, consumer, autoAck, false, false, false, nil)
}
