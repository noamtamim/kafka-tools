package main

import (
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"unicode/utf8"

	"github.com/IBM/sarama"
)

func main() {

	if len(os.Args) < 3 {
		usageAndExit()
	}

	brokers := os.Args[1]
	action := os.Args[2]

	brokersList := strings.Split(brokers, ",")

	if action == "list" {
		// Special case because it does not need a topic
		listTopics(brokersList)
		os.Exit(0)
	}

	if len(os.Args) < 4 {
		usageAndExit()
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
		deleteTopic(brokersList, topic, options)
	default:
		log.Panic("Unknown action")
	}
}

func usageAndExit() {
	fmt.Print(`
Usage: kafka-tools BROKERS ACTION [TOPIC] [OPTIONS]

Actions:
list [--system]
consume TOPIC [--raw] [--from-beginning]
produce TOPIC MESSAGE [--key KEY]
create TOPIC [--partitions PARTITIONS] [--replication-factor FACTOR] [--min-insync-replicas REPLICAS]
delete TOPIC [--yes]
`)
	os.Exit(1)
}

func jsonEncode(v any) string {
	j, _ := json.Marshal(v)
	return string(j)
}

func listTopics(brokers []string) {
	flagSet := flag.NewFlagSet("list", flag.ExitOnError)
	showSystemTopics := flagSet.Bool("system", false, "show system topics")
	_ = flagSet.Parse(os.Args[3:])

	admin, err := sarama.NewClusterAdmin(brokers, nil)
	if err != nil {
		log.Panic(err)
	}

	topics, err := admin.ListTopics()
	if err != nil {
		log.Panic(err)
	}

	for topic, details := range topics {
		if !*showSystemTopics && strings.HasPrefix(topic, "__") {
			continue
		}
		fmt.Println("Topic:", topic)
		fmt.Println("    ",
			"NumPartitions:", details.NumPartitions,
			"ReplicationFactor:", details.ReplicationFactor,
			"ReplicaAssignment:", jsonEncode(details.ReplicaAssignment),
			"ConfigEntries:", jsonEncode(details.ConfigEntries),
		)
	}

	err = admin.Close()
	if err != nil {
		log.Panic(err)
	}
}

func consumeTopic(brokers []string, topic string, options []string) {
	flagSet := flag.NewFlagSet("consume", flag.ExitOnError)
	rawOutput := flagSet.Bool("raw", false, "print raw values")
	fromBeginning := flagSet.Bool("from-beginning", false, "consume from the oldest offset")

	_ = flagSet.Parse(options)

	initialOffset := sarama.OffsetNewest
	if *fromBeginning {
		initialOffset = sarama.OffsetOldest
	}

	c, err := sarama.NewConsumer(brokers, nil)
	if err != nil {
		log.Panic(err)
	}

	partitionList, err := c.Partitions(topic)
	if err != nil {
		log.Panic(err)
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
			log.Panic(err)
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
			printMessage(msg, *rawOutput)
		}
	}()

	wg.Wait()
	log.Println("Done consuming topic", topic)
	close(messages)

	if err := c.Close(); err != nil {
		log.Println("Failed to close consumer: ", err)
	}
}

func printMessage(msg *sarama.ConsumerMessage, rawValue bool) {
	value, binaryValue := getString(msg.Value)
	key, binaryKey := getString(msg.Key)

	if rawValue {
		fmt.Println(value)

	} else {
		headers := make([]header, len(msg.Headers))
		for i, h := range msg.Headers {
			var key, binaryKey = getString(h.Key)
			var value, binaryValue = getString(h.Value)
			headers[i] = header{
				Key:         key,
				BinaryKey:   binaryKey,
				Value:       value,
				BinaryValue: binaryValue,
			}
		}
		fmt.Println(jsonEncode(&message{
			Value:       value,
			BinaryValue: binaryValue,
			Key:         key,
			BinaryKey:   binaryKey,
			//Topic:     msg.Topic,
			Partition: msg.Partition,
			Offset:    msg.Offset,
			Headers:   headers,
		}))
	}
}

type header struct {
	Key         string `json:"key"`
	BinaryKey   bool   `json:"binaryKey"`
	Value       string `json:"value"`
	BinaryValue bool   `json:"binaryValue"`
}
type message struct {
	Value       string   `json:"value"`
	BinaryValue bool     `json:"binaryValue"`
	Key         string   `json:"key"`
	BinaryKey   bool     `json:"binaryKey"`
	Partition   int32    `json:"partition"`
	Offset      int64    `json:"offset"`
	Headers     []header `json:"headers"`
}

func getString(value []byte) (string, bool) {
	if utf8.Valid(value) {
		return string(value), false
	} else {
		return base64.StdEncoding.EncodeToString(value), true
	}
}

func produceTopic(brokers []string, topic string, options []string) {
	flagSet := flag.NewFlagSet("produce", flag.ExitOnError)
	message := options[0]

	key := flagSet.String("key", "", "the message key")
	_ = flagSet.Parse(options[1:])

	producer, err := sarama.NewSyncProducer(brokers, nil)
	if err != nil {
		log.Panic(err)
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
		log.Panic(err)
	}

	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)
}

func createTopic(brokers []string, topic string, options []string) {
	flagSet := flag.NewFlagSet("create-topic", flag.ExitOnError)
	partitions := flagSet.Int("partitions", -1, "the number of partitions")
	replicationFactor := flagSet.Int("replication-factor", -1, "the replication factor")
	minInsyncReplicas := flagSet.Int("min-insync-replicas", 0, "the minimum number of in-sync replicas")
	_ = flagSet.Parse(options)

	adminClient, err := sarama.NewClusterAdmin(brokers, nil)
	if err != nil {
		log.Panic(err)
	}

	configEntries := make(map[string]*string)
	if *minInsyncReplicas > 0 {
		minInsyncReplicasString := strconv.Itoa(*minInsyncReplicas)
		configEntries["min.insync.replicas"] = &minInsyncReplicasString
	}

	err = adminClient.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     int32(*partitions),
		ReplicationFactor: int16(*replicationFactor),
		ConfigEntries:     configEntries,
	}, false)

	if err != nil {
		log.Panic("Failed to create topic:", err)
	}
	fmt.Println("Created topic", topic)
	defer func() {
		if err := adminClient.Close(); err != nil {
			log.Panic(err)
		}
	}()
}

func deleteTopic(brokers []string, topic string, options []string) {
	flagSet := flag.NewFlagSet("delete-topic", flag.ExitOnError)
	confirmed := flagSet.Bool("yes", false, "delete without asking")
	_ = flagSet.Parse(options)
	adminClient, err := sarama.NewClusterAdmin(brokers, nil)
	if err != nil {
		log.Panic(err)
	}

	if !*confirmed {
		// Prompt the user to confirm deletion
		fmt.Printf("Are you sure you want to delete topic %s? (y/N): ", topic)
		var response string
		_, err := fmt.Scanln(&response)
		if err != nil || response != "y" {
			fmt.Println("Aborting deletion")
			return
		}
	}

	err = adminClient.DeleteTopic(topic)
	if err != nil {
		log.Panic("Failed to delete topic:", err)
	}

	log.Println("Deleted topic", topic)
	defer func() {
		if err := adminClient.Close(); err != nil {
			log.Panic(err)
		}
	}()
}
