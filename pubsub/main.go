package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log/slog"
	// "log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"rmqkafka_pipeline/pubsub/cmd/channel"
	"rmqkafka_pipeline/pubsub/config"
	"runtime"
	"strings"

	"gopkg.in/yaml.v2"
)

// Queue represents the structure of a queue as returned by RabbitMQ API
type Queue struct {
	Name string `json:"name"`
}

type Exchange struct {
	Name string `json:"name"`
}

type Binding struct {
	Source          string `json:"source"`
	Destination     string `json:"destination"`
	DestinationType string `json:"destination_type"`
	RoutingKey      string `json:"routing_key"`
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

	// logging setup

	logFile, err := os.OpenFile("logs/app.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        panic(err)
    }
	
    defer logFile.Close()

	logger := slog.New(slog.NewTextHandler(logFile, nil))

	// load rabbitMQ config
	config_path := filepath.Join(basepath, "./config/", "rmq_config.yaml")
	f, err := os.Open(config_path)
	if err != nil {
		logger.Error("RMQ Configuration not found")
	}
	defer f.Close()

	var rmq_config config.RMQConfig
	decoder := yaml.NewDecoder(f)

	err = decoder.Decode(&rmq_config)
	if err != nil {
		logger.Error("Error decoding RMQ Configs")
	}

	// load kafka config
	config_path = filepath.Join(basepath, "./config/", "kafka_config.yaml")
	nf, err := os.Open(config_path)
	if err != nil {
		logger.Error("Kafka Configuration not found")
	}
	defer nf.Close()

	var kafka_config config.KafkaConfig
	kafkaDecoder := yaml.NewDecoder(nf)

	err = kafkaDecoder.Decode(&kafka_config)
	if err != nil {
		logger.Error("Error decoding Kafka Config")
	}

	rmqHost := strings.Split(rmq_config.Host, ":")[0]
	rabbitMgmtURL := fmt.Sprintf("http://%s:15672", rmqHost)
	
	exchanges, err := fetchExchanges(rabbitMgmtURL, rmq_config.Vhost, "amr", rmq_config.Username, rmq_config.Password)
	if err != nil {
		logger.Error("Error fetching exchanges", slog.String("error", err.Error()))
	}

	allBindings := make(map[string]map[string]string)
	for _, ex := range exchanges {
		bindings, err := fetchBindings(rabbitMgmtURL, rmq_config.Vhost, ex, rmq_config.Username, rmq_config.Password)
		if err != nil {
			logger.Error("Error fetching queue-routingkey bindings for exchange", slog.String("exchange", ex), slog.String("error", err.Error()))
			continue
		}
		allBindings[ex] = bindings
	}

	conductor, err := channel.NewConductor(rmq_config, kafka_config,logger)
	if err != nil {
		logger.Error("New Conductor Could not be created")
	}

	err = conductor.Start(allBindings)
	if err != nil {
		logger.Error("Conductor Failed")
	}
}
