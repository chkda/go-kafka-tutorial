package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {

	if len(os.Args) < 4 {
		fmt.Fprintf(os.Stderr, "Usage: %s <bootstrap-servers> <group> <topics..>\n", os.Args[0])
		os.Exit(1)
	}

	bootstrapServers := os.Args[1]
	group := os.Args[2]
	topics := os.Args[3:]

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        bootstrapServers,
		"broker.address.family":    "v4",
		"group.id":                 group,
		"session.timeout.ms":       6000,
		"auto.offset.reset":        "earliest",
		"enable.auto.offset.store": false,
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create a consumer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Consumer %v\n", consumer)

	err = consumer.SubscribeTopics(topics, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to subscribe to topics: %s\n", err)
		os.Exit(1)
	}
	run := true
	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			event := consumer.Poll(100)
			if event == nil {
				continue
			}

			switch msg := event.(type) {
			case *kafka.Message:
				fmt.Printf("Message on %s:\n%s\n", msg.TopicPartition, msg.Value)
				if msg.Headers != nil {
					fmt.Printf("Headers: %v\n", msg.Headers)
				}

				_, err := consumer.StoreMessage(msg)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error storing offset after message %s:\n", msg.TopicPartition)
				}
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "Error: %v: %v\n", msg.Code(), msg)
				if msg.Code() == kafka.ErrAllBrokersDown {
					run = false
				}
			default:
				fmt.Printf("Ignored %v\n", msg)
			}
		}
	}
	fmt.Printf("Closing Consumer\n")
	consumer.Close()
}
