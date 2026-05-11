package utils

import (
	"fmt"
	"github.com/IBM/sarama"
)

func EnsureTopicExists(brokers []string, topic string) error {
	config := sarama.NewConfig()
	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		return fmt.Errorf("error creating cluster admin: %v", err)
	}
	defer admin.Close()

	// Check if topic exists
	topics, err := admin.ListTopics()
	if err != nil {
		return fmt.Errorf("error listing topics: %v", err)
	}

	retentionMs := "5000"
	cleanupPolicy := "delete"

	if _, exists := topics[topic]; !exists {
		// Create topic
		err = admin.CreateTopic(topic, &sarama.TopicDetail{
			NumPartitions:     1,
			ReplicationFactor: 1,
			ConfigEntries: map[string]*string{
				"retention.ms":   &retentionMs,
				"cleanup.policy": &cleanupPolicy,
			},
		}, false)
		if err != nil {
			return fmt.Errorf("error creating topic %s: %v", topic, err)
		}
		fmt.Printf("Topic %s created with retention.ms=%s and cleanup.policy=%s\n", topic, retentionMs, cleanupPolicy)
	} else {
		// Update topic config if it already exists (to ensure it has the correct settings)
		err = admin.AlterConfig(sarama.TopicResource, topic, map[string]*string{
			"retention.ms":   &retentionMs,
			"cleanup.policy": &cleanupPolicy,
		}, false)
		if err != nil {
			return fmt.Errorf("error altering topic config for %s: %v", topic, err)
		}
		fmt.Printf("Topic %s config updated: retention.ms=%s, cleanup.policy=%s\n", topic, retentionMs, cleanupPolicy)
	}

	return nil
}
