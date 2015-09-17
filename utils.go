package epee

import (
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/samuel/go-zookeeper/zk"
	"io/ioutil"
	"log"
	"time"
)

var (
	ErrDecodingMessageFailed = errors.New("message decoding failed")
	ErrNotFound              = errors.New("not found")
	ErrStreamClosing         = errors.New("stream closing")
	ErrNoBrokers             = errors.New("no brokers found")

	// Logger that can be used whenever we want to log stuff. Gets discarded by
	// default.
	Logger *log.Logger
)

const (
	RetryForever = 0
)

func init() {
	Logger = log.New(ioutil.Discard, "", 0)
}

func logMessage(level, format string, args ...interface{}) {
	if Logger == nil {
		return
	}

	msg := fmt.Sprintf(format, args...)
	Logger.Printf("[epee] %s: %s", level, msg)
}

func logInfo(format string, args ...interface{}) {
	logMessage("INFO", format, args...)
}

func logError(format string, args ...interface{}) {
	logMessage("ERROR", format, args...)
}

func logWarning(format string, args ...interface{}) {
	logMessage("WARN", format, args...)
}

func logPanic(format string, args ...interface{}) {
	if Logger == nil {
		return
	}

	msg := fmt.Sprintf(format, args...)
	Logger.Panicf("[epee] PANIC: %s", msg)
}

// Must open a Zookeeper connection within retry times. If retry <= 0, it will
// retry for forever.
func MustGetZookeeperClient(servers []string, retry int) ZookeeperClient {
	var client ZookeeperClient
	attempts := 0

	for {
		var err error

		client, err = NewZookeeperClient(servers)

		// Increment retry if need be.
		if retry > 0 {
			attempts += 1
		}

		if err != nil && attempts > retry {
			panic(err)
		} else if err != nil {
			<-time.After(3 * time.Second)
		} else {
			// We found it, we're good!
			break
		}
	}

	return client
}

func findRegisteredBrokers(client ZookeeperClient) ([]string, error) {
	paths, err := client.List("/brokers/ids")

	if err == zk.ErrNoNode {
		return []string{}, ErrNoBrokers
	} else if err != nil {
		return []string{}, err
	}

	fullPaths := make([]string, 0)

	for _, p := range paths {
		data := make(map[string]interface{}, 0)
		err := client.Get(p, &data)

		if err != nil {
			return []string{}, err
		}

		fullPaths = append(fullPaths, fmt.Sprintf("%s:%0.0f", data["host"], data["port"]))
	}

	return fullPaths, nil
}

func getConfig(clientID string) *sarama.Config {
	config := sarama.NewConfig()
	config.Producer.Compression = sarama.CompressionSnappy
	config.ClientID = clientID
	config.Producer.Partitioner = func(topic string) sarama.Partitioner {
		return sarama.NewHashPartitioner(topic)
	}

	config.Producer.MaxMessageBytes = MaxMessageSize
	config.Consumer.Fetch.Max = int32(MaxMessageSize)

	// We bump the default ever-so-slightly just incase. This could be optimized
	// to do something better statistically.
	config.Consumer.Fetch.Default = int32(MaxMessageSize / 4)

	return config
}
