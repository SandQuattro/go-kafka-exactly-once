package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	ctx := context.Background()

	// Создание консьюмера
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  "localhost:9091,localhost:9092,localhost:9093",
		"group.id":           "my-group",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,            // !! Отключение авто-коммитов
		"isolation.level":    "read_committed", // !! чтение только зафиксированных данных
	})
	if err != nil {
		log.Fatalf("Failed to create consumer: %s", err)
	}

	defer consumer.Close()

	// Подписываемся на нужный топик
	err = consumer.Subscribe("test-topic", nil)
	if err != nil {
		log.Fatalf("Failed to subscribe to topic: %s", err)
	}

	// Создание продюсера с транзакциями
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":  "localhost:9091,localhost:9092,localhost:9093",
		"transactional.id":   "my-transactional-id",
		"acks":               "all",
		"enable.idempotence": true,
	})
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}

	defer producer.Close()

	// Инициализация транзакций
	if err = producer.InitTransactions(ctx); err != nil {
		log.Fatalf("Failed to initialize transactions: %s", err)
	}

	for {
		// Начало транзакции
		if err = producer.BeginTransaction(); err != nil {
			log.Fatalf("Failed to begin transaction: %s", err)
		}

		// Чтение сообщения из топика
		var msg *kafka.Message
		msg, err = consumer.ReadMessage(-1)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading message: %v\n", err)
			// Откат транзакции
			if abortErr := producer.AbortTransaction(ctx); abortErr != nil {
				log.Fatalf("Failed to abort transaction: %s", abortErr)
			}
			continue
		}

		fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))

		// Имитация обработки
		if string(msg.Value) == "error" { // Имитация ошибки
			fmt.Println("Simulating processing error.")
			// Откат транзакции
			if abortErr := producer.AbortTransaction(ctx); abortErr != nil {
				log.Fatalf("Failed to abort transaction: %s", abortErr)
			}
			continue
		}

		// Отправка обработанного сообщения в другой топик
		err = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: msg.TopicPartition.Topic, Partition: kafka.PartitionAny},
			Value:          msg.Value,
		}, nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to produce message: %v\n", err)
			// Откат транзакции
			if abortErr := producer.AbortTransaction(ctx); abortErr != nil {
				log.Fatalf("Failed to abort transaction: %s", abortErr)
			}
			continue
		}

		// Коммит чтения и записи как часть транзакции
		metadata, err := consumer.GetConsumerGroupMetadata()
		if err != nil {
			log.Fatalf("Failed to get consumer group metadata: %s", err)
		}
		offsets := []kafka.TopicPartition{{Topic: msg.TopicPartition.Topic, Partition: msg.TopicPartition.Partition, Offset: msg.TopicPartition.Offset + 1}}
		if err = producer.SendOffsetsToTransaction(ctx, offsets, metadata); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to send offsets to transaction: %v\n", err)
			// Откат транзакции
			if abortErr := producer.AbortTransaction(ctx); abortErr != nil {
				log.Fatalf("Failed to abort transaction: %s", abortErr)
			}
			continue
		}

		// Коммит транзакции
		if err := producer.CommitTransaction(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to commit transaction: %v\n", err)
			// Откат транзакции
			if abortErr := producer.AbortTransaction(ctx); abortErr != nil {
				log.Fatalf("Failed to abort transaction: %s", abortErr)
			}
		}
	}
}
