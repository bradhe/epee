package epee

import (
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
)

var (
	ErrDecodingMessageFailed = errors.New("message decoding failed")
	ErrNotFound              = errors.New("not found")
	ErrStreamClosing         = errors.New("stream closing")
)

func findRegisteredBrokers(zk ZookeeperClient) ([]string, error) {
	paths, err := zk.List("/brokers/ids")

	if err != nil {
		return []string{}, err
	}

	fullPaths := make([]string, 0)

	for _, p := range paths {
		data := make(map[string]interface{})
		err := zk.Get(p, data)

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

	return config
}
