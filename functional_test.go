package sarama

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/client"
)

const (
	VagrantToxiproxy      = "http://192.168.100.67:8474"
	VagrantKafkaPeers     = "192.168.100.67:9091,192.168.100.67:9092,192.168.100.67:9093,192.168.100.67:9094,192.168.100.67:9095"
	VagrantZookeeperPeers = "192.168.100.67:2181,192.168.100.67:2182,192.168.100.67:2183,192.168.100.67:2184,192.168.100.67:2185"
)

var (
	kafkaAvailable, kafkaRequired bool
	kafkaBrokers                  []string

	proxyClient *toxiproxy.Client
	Proxies     map[string]*toxiproxy.Proxy
)


func init() {
	if os.Getenv("DEBUG") == "true" {
		Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	seed := time.Now().UTC().UnixNano()
	if tmp := os.Getenv("TEST_SEED"); tmp != "" {
		seed, _ = strconv.ParseInt(tmp, 0, 64)
	}
	Logger.Println("Using random seed:", seed)
	rand.Seed(seed)

	proxyAddr := os.Getenv("TOXIPROXY_ADDR")
	if proxyAddr == "" {
		proxyAddr = VagrantToxiproxy
	}
	proxyClient = toxiproxy.NewClient(proxyAddr)

	kafkaPeers := os.Getenv("KAFKA_PEERS")
	if kafkaPeers == "" {
		kafkaPeers = VagrantKafkaPeers
	}
	kafkaBrokers = strings.Split(kafkaPeers, ",")

	if c, err := net.DialTimeout("tcp", kafkaBrokers[0], 5*time.Second); err == nil {
		if err = c.Close(); err == nil {
			kafkaAvailable = true
		}
	}

	kafkaRequired = os.Getenv("CI") != ""

	if kafkaAvailable {
		createTestTopics()
	}
}

func checkKafkaAvailability(t testing.TB) {
	if !kafkaAvailable {
		if kafkaRequired {
			t.Fatalf("Kafka broker is not available on %s. Set KAFKA_PEERS to connect to Kafka on a different location.", kafkaBrokers[0])
		} else {
			t.Skipf("Kafka broker is not available on %s. Set KAFKA_PEERS to connect to Kafka on a different location.", kafkaBrokers[0])
		}
	}
}

func checkKafkaVersion(t testing.TB, requiredVersion string) {
	kafkaVersion := os.Getenv("KAFKA_VERSION")
	if kafkaVersion == "" {
		t.Logf("No KAFKA_VERSION set. This test requires Kafka version %s or higher. Continuing...", requiredVersion)
	} else {
		available := parseKafkaVersion(kafkaVersion)
		required := parseKafkaVersion(requiredVersion)
		if !available.satisfies(required) {
			t.Skipf("Kafka version %s is required for this test; you have %s. Skipping...", requiredVersion, kafkaVersion)
		}
	}
}

func resetProxies(t testing.TB) {
	if err := proxyClient.ResetState(); err != nil {
		t.Error(err)
	}
	Proxies = nil
}

func fetchProxies(t testing.TB) {
	var err error
	Proxies, err = proxyClient.Proxies()
	if err != nil {
		t.Fatal(err)
	}
}

func SaveProxy(t *testing.T, px string) {
	if err := Proxies[px].Save(); err != nil {
		t.Fatal(err)
	}
}

func setupFunctionalTest(t testing.TB) {
	checkKafkaAvailability(t)
	resetProxies(t)
	fetchProxies(t)
}

func teardownFunctionalTest(t testing.TB) {
	resetProxies(t)
}

type kafkaVersion []int

func (kv kafkaVersion) satisfies(other kafkaVersion) bool {
	var ov int
	for index, v := range kv {
		if len(other) <= index {
			ov = 0
		} else {
			ov = other[index]
		}

		if v < ov {
			return false
		} else if v > ov {
			return true
		}
	}
	return true
}

func parseKafkaVersion(version string) kafkaVersion {
	numbers := strings.Split(version, ".")
	result := make(kafkaVersion, 0, len(numbers))
	for _, number := range numbers {
		nr, _ := strconv.Atoi(number)
		result = append(result, nr)
	}

	return result
}

func createTestTopics() {
	Logger.Println("Creating topics")
	config := NewConfig()
	config.Metadata.Retry.Max = 3
	config.Metadata.Retry.Backoff = 3 * time.Second
	var err error
	config.Version, err = ParseKafkaVersion(os.Getenv("KAFKA_VERSION"))
	if err != nil {
		panic(fmt.Sprintf("Could not parse kafka version %s: %+v", os.Getenv("KAFKA_VERSION"), err))
	}
	client, err := NewClient(kafkaBrokers, config)
	if err != nil {
		panic("failed to connect to kafka")
	}
	defer client.Close()
	broker, err := client.Controller()
	if err != nil {
		panic(fmt.Sprintf("no controller available: %+v", err))
	}

	// Delete the uncommitted-topic-test-4 topic (which is used by
	/// https://github.com/FrancoisPoinsot/simplest-uncommitted-msg/blob/master/src/main/java/CustomProducer/Main.java
	// in TestReadOnlyAndAllCommittedMessages), so we can re-create it empty, and run the test.k
	deleteRes, err := broker.DeleteTopics(&DeleteTopicsRequest{
		Topics: []string{"uncomitted-topic-test-4", "test.1", "test.4", "test.64"},
		Timeout: 3 * time.Second,
	})
	if err != nil {
		panic(fmt.Sprintf("Could not delete the uncomitted 4 topic: %+v", err))
	}
	for _, topicErr := range deleteRes.TopicErrorCodes {
		if topicErr != ErrUnknownTopicOrPartition && topicErr != ErrInvalidTopic && topicErr != ErrNoError {
			panic(fmt.Sprintf("failed to delete topic: %+v", topicErr))
		}
	}

	// We need to wait a while for the deletes to process
	topicsOk := false
mdloop:
	for i := 0; i < 20; i++{
		time.Sleep(1 * time.Second)
		md, err := broker.GetMetadata(&MetadataRequest{
			Topics: []string{"uncomitted-topic-test-4", "test.1", "test.4", "test.64"},
		})
		if err != nil {
			panic(fmt.Sprintf("failed to call GetMetadata: %+v", err))
		}
		for _, topicMd := range md.Topics {
			if topicMd.Err != ErrUnknownTopicOrPartition && topicMd.Err != ErrInvalidTopic && topicMd.Err != ErrNoError {
				// Need to try again
				continue mdloop
			}
		}
		topicsOk = true
		break
	}
	if !topicsOk {
		panic(fmt.Sprintf("timout waiting for topics to be OK"))
	}

	createRes, err := broker.CreateTopics(&CreateTopicsRequest{
		TopicDetails: map[string]*TopicDetail{
			"test.1": {
				NumPartitions: 1,
				ReplicationFactor: 3,
			},
			"test.4": {
				NumPartitions: 4,
				ReplicationFactor: 3,
			},
			"test.64": {
				NumPartitions: 64,
				ReplicationFactor: 3,
			},
			"uncommitted-topic-test-4": {
				NumPartitions: 1,
				ReplicationFactor: 3,
			},
		},
		Timeout: 3 * time.Second,
	})
	if err != nil {
		panic(fmt.Sprintf("could not create topics: %+v", err))
	}
	for topic, topicErr := range createRes.TopicErrors {
		if topicErr.Err != ErrTopicAlreadyExists && topicErr.Err != ErrNoError {
			panic(fmt.Sprintf("failed to create topic %s: %+v", topic, topicErr))
		}
	}

	// Now fill the topic with the java blob.
	c := exec.Command("wget", "-nc", "https://github.com/FrancoisPoinsot/simplest-uncommitted-msg/releases/download/0.1/simplest-uncommitted-msg-0.1-jar-with-dependencies.jar")
	c.Stderr = os.Stderr
	c.Stdout = os.Stdout
	err = c.Run()
	if err != nil {
		panic(fmt.Sprintf("failed to download jre blob: %+v", err))
	}
	c = exec.Command("java", "-jar", "simplest-uncommitted-msg-0.1-jar-with-dependencies.jar", "-b", kafkaBrokers[0], "-c", "4")
	c.Stderr = os.Stderr
	c.Stdout = os.Stdout
	err = c.Run()
	if err != nil {
		panic(fmt.Sprintf("failed to run java consumer: %+v", err))
	}

	Logger.Println("Created topics OK.")
}