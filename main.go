package main

import (
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
	idempotentProducer := idempotent.NewIdempotentProducer("localhost:9091", "localhost:9092", "localhost:9093")
	defer idempotentProducer.Close()

	err := idempotentProducer.ProduceMessage()
	if err != nil {
		log.Fatalf("Failed to produce message by idempotent producer: %s", err)
	}

	log.Printf("Messages were successfully produced by idempotent producer.")

}
