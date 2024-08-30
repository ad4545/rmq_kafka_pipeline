package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"rmqkafka_pipeline/pubsub/cmd/channel"
	"rmqkafka_pipeline/pubsub/config"
	"runtime"

	"gopkg.in/yaml.v2"
)

// Queue represents the structure of a queue as returned by RabbitMQ API
type Queue struct {
	Name string `json:"name"`
}

type Binding struct {
	Source          string `json:"source"`
	Destination     string `json:"destination"`
	DestinationType string `json:"destination_type"`
	RoutingKey      string `json:"routing_key"`
}

func fetchBindings(rabbitURL, vhost, exchange, username, password string) (map[string]string, error) {
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

func main() {

	_, b, _, _ := runtime.Caller(0)
	basepath := filepath.Dir(b)

	// load rabbitMQ config
	config_path := filepath.Join(basepath, "./config/", "rmq_config.yaml")
	f, err := os.Open(config_path)
	if err != nil {
		log.Fatalf("RMQ Configuration not found @ %s: %v", config_path, err)
	}
	defer f.Close()

	var rmq_config config.RMQConfig
	decoder := yaml.NewDecoder(f)

	err = decoder.Decode(&rmq_config)
	if err != nil {
		log.Fatalf("Error decoding RMQ Config from %s: %v", config_path, err)
	}

	// load kafka config
	config_path = filepath.Join(basepath, "./config/", "kafka_config.yaml")
	nf, err := os.Open(config_path)
	if err != nil {
		log.Fatalf("RMQ Configuration not found @ %s: %v", config_path, err)
	}
	defer nf.Close()

	var kafka_config config.KafkaConfig
	kafkaDecoder := yaml.NewDecoder(nf)

	err = kafkaDecoder.Decode(&kafka_config)
	if err != nil {
		log.Fatalf("Error decoding RMQ Config from %s: %v", config_path, err)
	}

	bindings, err := fetchBindings(rmq_config.Host, rmq_config.Vhost, "robot1", rmq_config.Username, rmq_config.Password)
	if err != nil {
		log.Fatalf("Error fetching queue-routingkey bindings: %v", err)
	}

	conductor, err := channel.NewConductor(rmq_config, kafka_config)
	if err != nil {
		log.Fatalf("New Conductor Could not be created: %v", err)
	}

	err = conductor.Start(bindings)
	if err != nil {
		log.Fatalf("Conductor Failed: %v", err)
	}

}
