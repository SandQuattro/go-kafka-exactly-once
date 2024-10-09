package idempotent

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"strings"
)

var topic = "test-topic"

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
