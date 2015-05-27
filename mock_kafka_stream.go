package epee

import (
	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
)

func newMockKafkaStream(t mocks.ErrorReporter, clientID string, zk ZookeeperClient) (kafkaStream, *mocks.Consumer) {
	config := sarama.NewConfig()
	config.ClientID = clientID

	// Now that we have a client, let's start a consumer up.
	consumer := mocks.NewConsumer(t, config)
	stream := new(kafkaStreamImpl)
	stream.client = nil
	stream.consumer = consumer
	stream.consumers = make(map[*streamConsumer]bool)

	return stream, consumer
}
