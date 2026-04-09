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
}

type Conductor interface {
	RunPipelines(*sync.WaitGroup)
	BuildPipelines() error
	// CheckForNewTopics(chan<- []string, chan int, config.RMQConfig)
	Start(Topics map[string]string) error
}

func NewConductor(rmqConfig config.RMQConfig, kafkaConfig config.KafkaConfig, logger *slog.Logger) (Conductor, error) {

	return &conductor{
		rmqConfig:   rmqConfig,
		kafkaConfig: kafkaConfig,
		logger: logger,
	}, nil
}

func (c *conductor) LoadKafkaRmqMapping(topics map[string]string) error {
	// Ensure the Topics map is initialized
	if c.internalState.Topics == nil {
		c.internalState.Topics = map[string]TopicMapping{}
	}

	// Static ID that will be prefixed
	staticID := "ccnt_robot1"

	// Iterate over the input topics array
	for queueName, routing_key := range topics {
		// Create the modified topic by replacing dots with underscores and prefixing with the static ID
		modifiedTopic := staticID + "_" + strings.ReplaceAll(queueName, ".", "_")

		// Get the corresponding routing key, if available

		// Store the original topic as the key and the struct as the value
		c.internalState.Topics[queueName] = TopicMapping{
			ModifiedTopic: modifiedTopic,
			RoutingKey:    routing_key,
		}
	}

	return nil
}

func (c *conductor) ConfigureBuilders() error {

	for rmq_topic, binding := range c.internalState.Topics {
		c.internalState.Configs[rmq_topic] = &config.RRPipelineConfig{
			RMQConnConfig: c.rmqConfig,
			RMQSubConfig: config.RMQClientConfig{
				Exchange:   "robot1",
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
			Name: rmq_topic,
		}
	}
	return nil
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

// func (c *conductor) CheckForNewTopics(topicStream chan<- []string, done chan int, rmq_config config.RMQConfig) {
// 	c.ticker = time.NewTicker(500 * time.Millisecond)

// 	defer c.ticker.Stop()
// 	defer c.waitGroup.Done()

// 	for {
// 		select {
// 		case <-c.ticker.C:
// 			bindings, err := fetchRoutingKeys(rmq_config.Host, rmq_config.Vhost, "robot1", rmq_config.Username, rmq_config.Password)
// 			if err != nil {
// 				c.errorChannels["self"] <- err
// 			}

// 			newQueues := []string{}
// 			for queue, routing_key := range bindings {
// 				if _, ok := c.internalState.Topics[queue]; !ok {
// 					newQueues = append(newQueues, queue)
// 					modifiedTopic := "1234" + "_" + strings.ReplaceAll(queue, ".", "_")

// 					// Store the original topic as the key and the struct as the value
// 					c.internalState.Topics[queue] = TopicMapping{
// 						ModifiedTopic: modifiedTopic,
// 						RoutingKey:    routing_key,
// 					}
// 				}
// 			}

// 			if len(newQueues) > 0 {
// 				topicStream <- newQueues
// 			}
// 		case <-done:
// 			return
// 		}
// 	}
// }

type S struct {
}
type P struct {
}

func (c *conductor) BuildPipelines() error {
	for config_name, conf := range c.internalState.Configs {
		if pipe, ok := c.internalState.Pipelines[config_name]; ok {
			if pipe.IsActive() {
				c.logger.Error("Pipeline is already active:",pipe.Name())
				continue
			} else {
				c.logger.Error("Pipeline is inactive",pipe.Name())
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

func (c *conductor) Start(Topics map[string]string) error {

	c.errorChannels = map[string]chan error{}
	c.errorChannels["self"] = make(chan error)
	c.internalState.Pipelines = map[string]iface.Pipeline{}
	c.internalState.Configs = map[string]*config.RRPipelineConfig{}

	err := c.LoadKafkaRmqMapping(Topics)

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

	// topicStream := make(chan []string)
	// done := make(chan int)
	c.waitGroup = &sync.WaitGroup{}

	// c.waitGroup.Add(1)
	// go func(topicS chan []string, done chan int, conductor *conductor) {
	// 	defer c.waitGroup.Done()
	// 	for {
	// 		select {
	// 		case topics := <-topicS:
	// 			log.Printf("New topic discovered %v\n", topics)
	// 			c.ConfigureBuilders()
	// 			c.BuildPipelines()
	// 			c.RunPipelines(c.waitGroup)
	// 		case err := <-c.errorChannels["self"]:
	// 			log.Printf("Error scanning for new topics: %v\n", err)
	// 		case <-done:
	// 			return
	// 		}
	// 	}
	// }(topicStream, done, c)

	// c.waitGroup.Add(1)
	// go c.CheckForNewTopics(topicStream, done, c.rmqConfig)

	//start pipelines here
	c.RunPipelines(c.waitGroup)

	c.waitGroup.Wait()
	return nil
}
