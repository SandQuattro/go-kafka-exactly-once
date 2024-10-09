package transactional

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"strings"
)

type TransactionalProducer struct {
	Producer *kafka.Producer
}

func NewTransactionalProducer(servers ...string) *TransactionalProducer {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": strings.Join(servers, ","),
		"transactional.id":  "my-transactional-id",
	})
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}
	return &TransactionalProducer{Producer: p}
}

func (tp *TransactionalProducer) ProduceMessage(topic string, keyF func() [20]string, message string) error {
	// Initialize transactions
	err := tp.Producer.InitTransactions(context.Background())
	if err != nil {
		log.Fatalf("Failed to initialize transactions: %s", err)
	}

	// Start a transaction
	err = tp.Producer.BeginTransaction()
	if err != nil {
		log.Fatalf("Failed to begin transaction: %s", err)
	}

	// Produce message
	for i := 0; i < 10; i++ {
		err = tp.Producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			// if key is not specified, then we use default partition distribution round-robin strategy
			Key:   []byte(keyF()[i%20]),
			Value: []byte(fmt.Sprintf("%s-%d", message, i)),
		}, nil)
		if err != nil {
			log.Println("Failed to produce message:", err)
			return err
		}
		log.Println("Produced message:", fmt.Sprintf("message-%d", i))
	}

	// Commit transaction
	err = tp.Producer.CommitTransaction(context.Background())
	if err != nil {
		log.Fatalf("Failed to commit transaction: %s", err)
	}

	return nil
}

func (tp *TransactionalProducer) Close() {
	tp.Producer.Flush(5000)
	tp.Producer.Close()
}
