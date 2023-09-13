package main

import (
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/IBM/sarama"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
)

func main() {

	if len(os.Args) < 3 {
		usage()
	}

	action := os.Args[1]
	brokers := os.Args[2]
	brokersList := strings.Split(brokers, ",")

	if action == "list" {
		// Special case because it does not need a topic
		listTopics(brokersList)
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
		consumeTopic(brokersList, topic, options)
	case "produce":
		produceTopic(brokersList, topic, options)
	case "create":
		createTopic(brokersList, topic, options)
	case "delete":
		deleteTopic(brokersList, topic)
	default:
		panic("Unknown action")
	}
}

func usage() {
	fmt.Println(`Usage: kafka-cli <action> <brokers> [topic] [options]`)
	fmt.Println(`Actions:`)
	fmt.Println(`  list`)
	fmt.Println(`  consume`)
	fmt.Println(`  produce`)
	fmt.Println(`  create`)
	fmt.Println(`  delete`)
	fmt.Println(`All actions except list require the topic.`)
	os.Exit(1)
}

func listTopics(brokers []string) {
	admin, err := sarama.NewClusterAdmin(brokers, nil)
	if err != nil {
		panic(err)
	}

	topics, err := admin.ListTopics()
	if err != nil {
		panic(err)
	}

	for topic, details := range topics {
		fmt.Println("Topic:", topic, "NumPartitions:", details.NumPartitions)
	}

	err = admin.Close()
	if err != nil {
		panic(err)
	}
}

func consumeTopic(brokers []string, topic string, options []string) {
	flagSet := flag.NewFlagSet("consume", flag.ExitOnError)
	rawValue := flagSet.Bool("raw", false, "only print raw values (no partition, offset, key)")
	binary := flagSet.Bool("binary", false, "expect binary data and print it as base64")
	binaryKey := flagSet.Bool("binary-key", false, "expect binary key and print it as base64")
	fromBeginning := flagSet.Bool("from-beginning", false, "whether to consume from the beginning")

	_ = flagSet.Parse(options)

	initialOffset := sarama.OffsetNewest
	if *fromBeginning {
		initialOffset = sarama.OffsetOldest
	}

	c, err := sarama.NewConsumer(brokers, nil)
	if err != nil {
		panic(err)
	}

	partitionList, err := c.Partitions(topic)
	if err != nil {
		panic(err)
	}

	var (
		messages = make(chan *sarama.ConsumerMessage, 256)
		closing  = make(chan struct{})
		wg       sync.WaitGroup
	)

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGTERM, os.Interrupt)
		<-signals
		log.Println("Initiating shutdown of consumer...")
		close(closing)
	}()

	for _, partition := range partitionList {
		pc, err := c.ConsumePartition(topic, partition, initialOffset)
		if err != nil {
			panic(err)
		}

		go func(pc sarama.PartitionConsumer) {
			<-closing
			pc.AsyncClose()
		}(pc)

		wg.Add(1)
		go func(pc sarama.PartitionConsumer) {
			defer wg.Done()
			for message := range pc.Messages() {
				messages <- message
			}
		}(pc)
	}

	go func() {
		for msg := range messages {
			value := getValue(msg.Value, *binary)
			key := getValue(msg.Key, *binaryKey)

			if *rawValue {
				fmt.Println(value)

			} else {
				data, err := json.Marshal(&map[string]any{
					"partition": msg.Partition,
					"offset":    msg.Offset,
					"key":       key,
					"value":     value,
				})
				if err != nil {
					panic(err)
				}

				fmt.Println(string(data))
			}
		}
	}()

	wg.Wait()
	log.Println("Done consuming topic", topic)
	close(messages)

	if err := c.Close(); err != nil {
		log.Println("Failed to close consumer: ", err)
	}
}

func getValue(value []byte, binary bool) string {
	if binary {
		return base64.StdEncoding.EncodeToString(value)
	} else {
		return string(value)
	}
}

func produceTopic(brokers []string, topic string, options []string) {
	flagSet := flag.NewFlagSet("produce", flag.ExitOnError)
	message := options[0]

	key := flagSet.String("key", "", "the message key")
	_ = flagSet.Parse(options[1:])

	producer, err := sarama.NewSyncProducer(brokers, nil)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			log.Panic(err)
		}
	}()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
		Key:   sarama.StringEncoder(*key),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)
}

func createTopic(brokers []string, topic string, options []string) {
	flagSet := flag.NewFlagSet("create-topic", flag.ExitOnError)
	partitions := flagSet.Int("partitions", 1, "the number of partitions")
	replicationFactor := flagSet.Int("replication-factor", 1, "the replication factor")
	_ = flagSet.Parse(options)

	adminClient, err := sarama.NewClusterAdmin(brokers, nil)
	if err != nil {
		log.Panic(err)
	}

	err = adminClient.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     int32(*partitions),
		ReplicationFactor: int16(*replicationFactor),
	}, false)

	if err != nil {
		fmt.Println("Failed to create topic:", err)
		return
	}
	fmt.Println("Created topic", topic)
	defer func() {
		if err := adminClient.Close(); err != nil {
			log.Panic(err)
		}
	}()
}

func deleteTopic(brokers []string, topic string) {
	adminClient, err := sarama.NewClusterAdmin(brokers, nil)
	if err != nil {
		log.Panic(err)
	}

	err = adminClient.DeleteTopic(topic)
	if err != nil {
		log.Println("Failed to delete topic:", err)
		return
	}

	log.Println("Deleted topic", topic)
	defer func() {
		if err := adminClient.Close(); err != nil {
			log.Panic(err)
		}
	}()
}
