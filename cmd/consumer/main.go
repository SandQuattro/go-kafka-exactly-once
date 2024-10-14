package main

import (
	"errors"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
)

func main() {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9091,localhost:9092,localhost:9093",
		"group.id":          "my-group-id",
		"auto.offset.reset": "earliest",
		"isolation.level":   "read_committed",
	})
	if err != nil {
		log.Fatalf("Failed to create consumer: %s", err)
	}
	defer func() {
		err = errors.Join(err, c.Close())
	}()

	topic := "test-topic"
	err = c.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		log.Fatalf("Failed to subscribe to topics: %s", err)
	}

	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			log.Printf("Message on %s: %s", msg.TopicPartition, string(msg.Value))
			// Process message here

			// Commit offsets
			offsets := []kafka.TopicPartition{msg.TopicPartition}
			_, err = c.CommitOffsets(offsets)
			if err != nil {
				log.Fatalf("Failed to commit offsets: %s", err)
			}
			log.Printf("Offsets committed for %v", offsets)
		} else {
			log.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}
}
