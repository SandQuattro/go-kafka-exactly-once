package idempotent

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"strings"
)

type IdempotentProducer struct {
	Producer *kafka.Producer
}

func NewIdempotentProducer(servers ...string) *IdempotentProducer {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":  strings.Join(servers, ","),
		"enable.idempotence": true,
		"acks":               "all",
	})
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}
	return &IdempotentProducer{
		p,
	}
}

func (ip *IdempotentProducer) Close() {
	ip.Producer.Flush(5000)
	ip.Producer.Close()
}

func (ip *IdempotentProducer) ProduceMessage(topic string, keyF func() [20]string, message string) error {
	// idempotency check
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(message),
	}

	for i := 0; i < 10; i++ {
		// if key is not specified, then we use default partition distribution round-robin strategy
		//this is for better partition distribution
		msg.Key = []byte(keyF()[i%20])

		err := ip.Producer.Produce(msg, nil)
		if err != nil {
			log.Println("Failed to produce message:", err)
			return err
		}
		log.Println("Produced message:", message)
	}

	return nil
}
