package main

import (
	"fmt"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintf(os.Stderr, "Usage: %s <bootstrap-servers> <topic>", os.Args[0])
	}

	bootstrapServers := os.Args[1]
	topic := os.Args[2]
	totalMsgcnt := 100

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
	})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	fmt.Print("Created producer %v\n", producer)

	go func() {
		for e := range producer.Events() {
			switch event := e.(type) {
			case kafka.Error:
				fmt.Printf("Error: %v\n", event)
			default:
				fmt.Printf("Ignored event: %s\n", event)
			}
		}
	}()

	msgcnt := 0
	for msgcnt < totalMsgcnt {
		value := fmt.Sprintf("Producer example, message #%d", msgcnt)
		deliveryChannel := make(chan kafka.Event)

		go func() {
			for e := range deliveryChannel {
				switch event := e.(type) {
				case *kafka.Message:
					msg := event
					if msg.TopicPartition.Error != nil {
						fmt.Printf("Delivery failed: %v\n", msg.TopicPartition.Error)
					} else {
						fmt.Printf("Delivered Message to topic %s [%d] at offset %v\n", *&msg.TopicPartition.Topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset)
					}
				default:
					fmt.Printf("Ignored event: %s\n", event)
				}
				close(deliveryChannel)
			}
		}()

		err := producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(value),
			Headers:        []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
		}, deliveryChannel)

		if err != nil {
			close(deliveryChannel)
			if err.(kafka.Error).Code() == kafka.ErrQueueFull {
				time.Sleep(time.Second)
				continue
			}
			fmt.Printf("Failed to produce message: %v\n", err)
		}
		msgcnt++
	}

	for producer.Flush(10000) > 0 {
		fmt.Print("Still waiting to flush outstanding messages\n", err)
	}
	producer.Close()
}
