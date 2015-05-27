package epee

import (
	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
)

// StreamProducers are used to publish protocol buffer messages to a Kafka
// stream on the topic of your choice. By default, epee uses a Hash partitioner
// for messages. This means that all messages with the same key will end up in
// the same partition in Kafka. This is the primary mechanism by which epee
// guarantees worker affinity for messages.
type StreamProducer struct {
	client sarama.Client

	producer sarama.AsyncProducer
}

// Serializes a protocol buffer message to a byte string and publishes it to
// the given topic with the given key. Hash partitioner is used during
// publishing to determine the partition the message should go in based on the
// key supplied.
func (sp *StreamProducer) Publish(topic, key string, message proto.Message) error {
	m := sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: newProtobufMessageEncoder(message),
	}

	sp.producer.Input() <- &m

	select {
	case err := <-sp.producer.Errors():
		return err.Err
	default:
		// Don't do anything.
	}

	return nil
}

// Creates a new StreamProducer. Looks up the brokers' addresses in Zookeeper
// to figure out where to connect to. If the brokers aren't found in zookeeper,
// this is considered an error.
func NewStreamProducer(clientID string, zk ZookeeperClient) (*StreamProducer, error) {
	brokers, err := findRegisteredBrokers(zk)

	if err != nil {
		return nil, err
	}

	client, err := sarama.NewClient(brokers, getConfig(clientID))

	if err != nil {
		return nil, err
	}

	producer, err := sarama.NewAsyncProducerFromClient(client)

	if err != nil {
		client.Close()
		return nil, err
	}

	sp := new(StreamProducer)
	sp.client = client
	sp.producer = producer

	return sp, nil
}
