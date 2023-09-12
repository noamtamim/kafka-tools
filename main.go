package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"os"
)

func main() {

	if len(os.Args) < 3 {
		fmt.Println("You need to specify at least action, brokers. Most actions also require a topic.")
		os.Exit(1)
	}

	action := os.Args[1]
	brokers := os.Args[2]

	config := &kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"client.id":         "kafka-tools",
	}

	if action == "list" {
		// Special case because it does not need a topic
		listTopics(config)
		os.Exit(0)
	}

	if len(os.Args) < 4 {
		fmt.Println("After actions and brokers, you need to specify a topic.")
		os.Exit(1)
	}

	topic := os.Args[3]
	options := os.Args[4:]

	switch action {
	case "consume":
		consumeTopic(config, topic, options)
	case "produce":
		produceTopic(config, topic, options)
	case "create":
		createTopic(config, topic, options)
	case "delete":
		deleteTopic(config, topic)
	default:
		panic("Unknown action")
	}
}

func listTopics(config *kafka.ConfigMap) {
	adminClient, err := kafka.NewAdminClient(config)
	if err != nil {
		panic(err)
	}

	res, err := adminClient.GetMetadata(nil, true, 1000)
	if err != nil {
		panic(err)
	}
	for _, metadata := range res.Topics {
		fmt.Println(metadata.Topic, len(metadata.Partitions))
	}

	adminClient.Close()
}

func consumeTopic(config *kafka.ConfigMap, topic string, options []string) {
	flagSet := flag.NewFlagSet("consume", flag.ExitOnError)
	fromBeginning := flagSet.Bool("from-beginning", false, "whether to consume from the beginning")
	groupId := flagSet.String("group-id", "kafka-tools", "the group id to use")
	if fromBeginning != nil && *fromBeginning {
		_ = config.SetKey("auto.offset.reset", kafka.OffsetBeginning.String())
	}
	_ = config.SetKey("group.id", *groupId)
	_ = flagSet.Parse(options)

	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		panic(err)
	}

	err = consumer.Subscribe(topic, nil)
	if err != nil {
		panic(err)
	}

	for {
		msg, err := consumer.ReadMessage(-1)
		if err != nil {
			panic(err)
		}

		log.Printf("[%d]: %s", msg.TopicPartition.Partition, string(msg.Value))
	}
}

func produceTopic(config *kafka.ConfigMap, topic string, options []string) {
	flagSet := flag.NewFlagSet("produce", flag.ExitOnError)
	message := options[0]

	key := flagSet.String("key", "", "the message key")
	_ = flagSet.Parse(options[1:])

	producer, err := kafka.NewProducer(config)
	if err != nil {
		panic(err)
	}

	deliveryChan := make(chan kafka.Event)
	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: []byte(message),
		Key:   []byte(*key),
	}, deliveryChan)
	if err != nil {
		panic(err)
	}

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		log.Printf("Delivery failed: %v", m.TopicPartition.Error)
	} else {
		log.Printf("Delivered message to partition %d", m.TopicPartition.Partition)
	}

	close(deliveryChan)
}

func createTopic(config *kafka.ConfigMap, topic string, options []string) {
	flagSet := flag.NewFlagSet("create-topic", flag.ExitOnError)
	partitions := flagSet.Uint("partitions", 1, "the number of partitions")
	replicationFactor := flagSet.Uint("replication-factor", 1, "the replication factor")
	_ = flagSet.Parse(options)

	adminClient, err := kafka.NewAdminClient(config)
	if err != nil {
		panic(err)
	}

	res, err := adminClient.CreateTopics(context.Background(), []kafka.TopicSpecification{
		{
			Topic:             topic,
			NumPartitions:     int(*partitions),
			ReplicationFactor: int(*replicationFactor),
		},
	})
	if err != nil {
		panic(err)
	}

	for _, r := range res {
		if r.Error.Code() != kafka.ErrNoError {
			log.Println(r.Error)
		} else {
			log.Println("Created topic", r.Topic)
		}
	}

	adminClient.Close()
}

func deleteTopic(config *kafka.ConfigMap, topic string) {
	adminClient, err := kafka.NewAdminClient(config)
	if err != nil {
		panic(err)
	}

	res, err := adminClient.DeleteTopics(context.Background(), []string{
		topic,
	})
	if err != nil {
		panic(err)
	}

	for _, r := range res {
		if r.Error.Code() != kafka.ErrNoError {
			log.Println(r.Error)
		} else {
			log.Println("Deleted topic", r.Topic)
		}
	}

	adminClient.Close()
}
