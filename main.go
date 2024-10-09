package main

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"kafka-exactly-once/cmd/producer/idempotent"
	"kafka-exactly-once/cmd/producer/transactional"
	"log"
)

func main() {
	runInTransaction()
	runInIdempotentMode()
}

func runInTransaction() {
	transactionalProducer := transactional.NewTransactionalProducer("localhost:9091", "localhost:9092", "localhost:9093")
	defer transactionalProducer.Close()

	err := transactionalProducer.ProduceMessage("test", "test-topic")
	if err != nil {
		log.Fatalf("Failed to produce message: %s", err)
	}

	log.Printf("Messages were successfully produced in a transaction.")
}

func runInIdempotentMode() {
	p := idempotent.NewIdempotentProducer("localhost:9091", "localhost:9092", "localhost:9093")
	defer p.Close()

	// idempotency check
	txt := "same message many times"
	topic := "test-topic"
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte("key"),
		Value:          []byte(txt),
	}
	for i := 0; i < 10; i++ {
		err := p.Producer.Produce(msg, nil)
		if err != nil {
			log.Println("Failed to produce message:", err)
			return
		}
		log.Println("Produced message:", txt)
	}

}
