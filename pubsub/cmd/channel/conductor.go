package channel

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	// "log"
	"net/http"
	"log/slog"
	"net/url"
	"rmqkafka_pipeline/pubsub/config"
	"rmqkafka_pipeline/pubsub/iface"

	// "log"
	"strings"

	// "log"
	"sync"
	"time"
)

type Binding struct {
	Source          string `json:"source"`
	Destination     string `json:"destination"`
	DestinationType string `json:"destination_type"`
	RoutingKey      string `json:"routing_key"`
}

type TopicMapping struct {
	ModifiedTopic string
	RoutingKey    string
}

type Queue struct {
	Name string `json:"name"`
}

type State struct {
	Topics map[string]TopicMapping

	Builders map[string]iface.Builder

	Pipelines map[string]iface.Pipeline

	Configs map[string]*config.RRPipelineConfig

	ValidTopics map[string]struct{}

	UserRequestedTopics map[string]struct{}
}

type conductor struct {

	//active internal state of the topics and their pipelines/builders maintained by the conductor
	internalState State

	//error channels per pipeline
	errorChannels map[string]chan error

	//wait group for pipelines
	waitGroup *sync.WaitGroup

	//node used by the conductor to query info regarding the topics and nodes
	kafkaConfig config.KafkaConfig

	rmqConfig config.RMQConfig
	//used by conductor discovery goroutine to publish new TopicInfo

	ticker *time.Ticker

	logger *slog.Logger

	mu sync.RWMutex
}

type Conductor interface {
	RunPipelines(*sync.WaitGroup)
	BuildPipelines() error
	// CheckForNewTopics(chan<- []string, chan int, config.RMQConfig)
	Start(exchangeBindings map[string]map[string]string) error
}

func NewConductor(rmqConfig config.RMQConfig, kafkaConfig config.KafkaConfig, logger *slog.Logger) (Conductor, error) {

	return &conductor{
		rmqConfig:   rmqConfig,
		kafkaConfig: kafkaConfig,
		logger: logger,
	}, nil
}

func (c *conductor) LoadKafkaRmqMapping(exchangeBindings map[string]map[string]string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Ensure the Topics map is initialized
	if c.internalState.Topics == nil {
		c.internalState.Topics = map[string]TopicMapping{}
	}

	// Iterate over the nested exchange bindings
	for exchange, topics := range exchangeBindings {
		for queueName, routing_key := range topics {
			// Create the modified topic by removing .q from the end of the queueName
			modifiedTopic := strings.TrimSuffix(queueName, ".q")

			// Use compound key to prevent collisions if different exchanges bind to same queue
			compoundKey := exchange + ":" + queueName

			// Store the original topic as the key and the struct as the value
			c.internalState.Topics[compoundKey] = TopicMapping{
				ModifiedTopic: modifiedTopic,
				RoutingKey:    routing_key,
			}
		}
	}

	return nil
}

func (c *conductor) ConfigureBuilders() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for compoundKey, binding := range c.internalState.Topics {
		parts := strings.SplitN(compoundKey, ":", 2)
		if len(parts) != 2 {
			continue
		}
		exchange := parts[0]
		rmq_topic := parts[1]

		c.internalState.Configs[compoundKey] = &config.RRPipelineConfig{
			RMQConnConfig: c.rmqConfig,
			RMQSubConfig: config.RMQClientConfig{
				Exchange:   exchange,
				Topic:      rmq_topic,
				RoutingKey: binding.RoutingKey,
				Durable:    true,
				Autodelete: false,
			},
			KafkaConfig: config.KafkaPublisherConfig{
				Topic: binding.ModifiedTopic,
				Name:  binding.ModifiedTopic, // Setting the Kafka topic name same as the channel name
			},
			KafkaConnConfig: config.KafkaConfig{
				Brokers: c.kafkaConfig.Brokers,
			},
			Name: compoundKey,
		}
	}
	return nil
}

type Exchange struct {
	Name string `json:"name"`
}

func fetchExchanges(rabbitURL, vhost, prefix, username, password string) ([]string, error) {
	encodedVhost := url.PathEscape(vhost)
	apiURL := fmt.Sprintf("%s/api/exchanges/%s", rabbitURL, encodedVhost)
	client := &http.Client{}
	req, err := http.NewRequest("GET", apiURL, nil)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(username, password)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var exchanges []Exchange
	if err := json.Unmarshal(body, &exchanges); err != nil {
		return nil, err
	}

	var matched []string
	for _, ex := range exchanges {
		if strings.HasPrefix(ex.Name, prefix) {
			matched = append(matched, ex.Name)
		}
	}

	return matched, nil
}

func fetchRoutingKeys(rabbitURL, vhost, exchange, username, password string) (map[string]string, error) {
	// Encode the vhost to handle special characters like "/"
	encodedVhost := url.PathEscape(vhost)
	// Encode the exchange to handle special characters
	encodedExchange := url.PathEscape(exchange)

	// Prepare the HTTP request URL with vhost and exchange
	apiURL := fmt.Sprintf("%s/api/exchanges/%s/%s/bindings/source", rabbitURL, encodedVhost, encodedExchange)
	client := &http.Client{}
	req, err := http.NewRequest("GET", apiURL, nil)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(username, password)

	// Send the request
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Read the response body
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// Debug: Print the raw response body
	// fmt.Println("Response Body:", string(body))

	// Parse the JSON response
	var bindings []Binding
	if err := json.Unmarshal(body, &bindings); err != nil {
		return nil, err
	}

	// Extract routing keys
	routingKeys := make(map[string]string)
	for _, binding := range bindings {
		routingKeys[binding.Destination] = binding.RoutingKey
	}

	return routingKeys, nil
}

func (c *conductor) CheckForNewTopics(topicStream chan<- map[string]map[string]string, done chan int, rmq_config config.RMQConfig) {
	c.ticker = time.NewTicker(15 * time.Second)

	defer c.ticker.Stop()
	defer c.waitGroup.Done()

	rmqHost := strings.Split(rmq_config.Host, ":")[0]
	rabbitMgmtURL := fmt.Sprintf("http://%s:15672", rmqHost)

	for {
		select {
		case <-c.ticker.C:
			exchanges, err := fetchExchanges(rabbitMgmtURL, rmq_config.Vhost, "amr", rmq_config.Username, rmq_config.Password)
			if err != nil {
				c.logger.Error("Error fetching exchanges in CheckForNewTopics", slog.String("error", err.Error()))
				continue
			}

			newBindings := make(map[string]map[string]string)
			hasNew := false

			for _, ex := range exchanges {
				bindings, err := fetchRoutingKeys(rabbitMgmtURL, rmq_config.Vhost, ex, rmq_config.Username, rmq_config.Password)
				if err != nil {
					c.logger.Error("Error fetching bindings in CheckForNewTopics", slog.String("exchange", ex), slog.String("error", err.Error()))
					continue
				}

				c.mu.RLock()
				for queueName, routing_key := range bindings {
					compoundKey := ex + ":" + queueName
					if _, ok := c.internalState.Topics[compoundKey]; !ok {
						if newBindings[ex] == nil {
							newBindings[ex] = make(map[string]string)
						}
						newBindings[ex][queueName] = routing_key
						hasNew = true
					}
				}
				c.mu.RUnlock()
			}

			if hasNew {
				topicStream <- newBindings
			}
		case <-done:
			return
		}
	}
}

type S struct {
}
type P struct {
}

func (c *conductor) BuildPipelines() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for config_name, conf := range c.internalState.Configs {
		if pipe, ok := c.internalState.Pipelines[config_name]; ok {
			if pipe.IsActive() {
				// c.logger.Info("Pipeline is already active:", slog.String("name", pipe.Name()))
				continue
			} else {
				c.logger.Error("Pipeline is inactive", slog.String("name", pipe.Name()))
			}
		}

		p, err := NewPipeline[S, P](*conf)
		if err != nil { 
			c.logger.Error("failed to build pipeline for", config_name, err)
			return err
		}
		c.internalState.Pipelines[config_name] = p
		c.logger.Info("Pipeline started for",slog.String("topic",config_name))
		c.errorChannels[config_name] = c.internalState.Pipelines[config_name].GetErrorStream()
	}

	return nil
}

func (c *conductor) RunPipelines(wg *sync.WaitGroup) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, pipeline := range c.internalState.Pipelines {
		if !pipeline.IsActive() {
			c.errorChannels[pipeline.Name()] = pipeline.GetErrorStream()
			c.waitGroup.Add(1)
			go func(errCh chan error) {
				for {
					select {
					case err := <-errCh:
						c.logger.Error("Error on:", pipeline.Name(), err)
					}
				}
			}(c.errorChannels[pipeline.Name()])
			c.waitGroup.Add(1)
			go pipeline.Start(c.waitGroup)
		}
	}
}

func (c *conductor) Start(exchangeBindings map[string]map[string]string) error {

	c.errorChannels = map[string]chan error{}
	c.errorChannels["self"] = make(chan error)
	c.internalState.Pipelines = map[string]iface.Pipeline{}
	c.internalState.Configs = map[string]*config.RRPipelineConfig{}

	err := c.LoadKafkaRmqMapping(exchangeBindings)

	if err != nil {
		c.logger.Error("Error at loading kafkaRMQMapping")
		return err
	}

	err = c.ConfigureBuilders()
	if err != nil {
		c.logger.Error("Error at configuring buidlers")
		return err
	}

	err = c.BuildPipelines()
	if err != nil {
		c.logger.Error("Error at building pipelines")
		return err
	}

	topicStream := make(chan map[string]map[string]string)
	done := make(chan int)
	c.waitGroup = &sync.WaitGroup{}

	c.waitGroup.Add(1)
	go func(topicS chan map[string]map[string]string, done chan int, conductor *conductor) {
		defer c.waitGroup.Done()
		for {
			select {
			case topics := <-topicS:
				c.logger.Info("New bindings discovered", slog.Any("bindings", topics))
				c.LoadKafkaRmqMapping(topics)
				c.ConfigureBuilders()
				c.BuildPipelines()
				c.RunPipelines(c.waitGroup)
			case err := <-c.errorChannels["self"]:
				c.logger.Error("Error scanning for new topics", slog.String("error", err.Error()))
			case <-done:
				return
			}
		}
	}(topicStream, done, c)

	c.waitGroup.Add(1)
	go c.CheckForNewTopics(topicStream, done, c.rmqConfig)

	//start pipelines here
	c.RunPipelines(c.waitGroup)

	c.waitGroup.Wait()
	return nil
}
