package epee

import (
	"github.com/Shopify/sarama"
	"log"
	"sync"
	"time"
)

type kafkaStream interface {
	// Close all resources associated with this thing.
	Close()

	// Returns a channel of messages to consume based on the client ID.
	Consume(topic string, partition int, offset int64) (*streamConsumer, error)

	// Given a consumer, gracefully stops it.
	CancelConsumer(*streamConsumer) error
}

type kafkaStreamImpl struct {
	sync.Mutex

	// A list of stream consumers that have been created.
	consumers map[*streamConsumer]bool

	// Indicates to child processes that we should continue running.
	closing bool

	// The consumer we're using to consume stuff.
	consumer sarama.Consumer

	// The client connected to
	client sarama.Client

	// The zookeeper cluster our service is connecting to.
	zk ZookeeperClient
}

func (ks *kafkaStreamImpl) Consume(topic string, partition int, offset int64) (*streamConsumer, error) {
	// If the stream is in the process of closing we don't want to start a new
	// consumer.
	if ks.closing {
		return nil, ErrStreamClosing
	}

	// Before we get started let's see if the topic actually exists.
	err := ks.client.RefreshMetadata(topic)

	// If we can't get metadata for a given topic we'll need to report that to
	// someone.
	if err != nil {
		return nil, err
	}

	if offset == 0 {
		offset = sarama.OffsetOldest
	}

	var partitionConsumer sarama.PartitionConsumer

	for {
		if partitionConsumer != nil {
			break
		}

		partitionConsumer, err = ks.consumer.ConsumePartition(topic, int32(partition), offset)

		if err == sarama.ErrUnknownTopicOrPartition {
			log.Printf("WARNING: Failed to find [%s, partition %d]. Waiting, then retrying.", topic, partition)
			<-time.After(5 * time.Second)
			continue
		} else if err != nil {
			log.Printf("ERROR: Failed to start partition consumer. %v", err)
			return nil, err
		}
	}

	ch := make(chan Message, 0)
	consumer := newStreamConsumer(ch, partitionConsumer)

	// We have to acquire the lock to modify the map.
	ks.Lock()
	ks.consumers[consumer] = true
	ks.Unlock()

	// Let's start the consumer up!
	consumer.Start()

	return consumer, nil
}

func (ks *kafkaStreamImpl) CancelConsumer(sc *streamConsumer) error {
	ks.Lock()
	defer ks.Unlock()

	// Only actually cancel this consumer if it's still alive.
	_, ok := ks.consumers[sc]

	if ok {
		sc.Close()
		delete(ks.consumers, sc)
	}

	return nil
}

func (ks *kafkaStreamImpl) Close() {
	ks.Lock()
	defer ks.Unlock()

	ks.closing = true

	// Let's close all the created consumers.
	for c := range ks.consumers {
		// Wait for this consumer to close fully.
		c.Close()
	}

	// Now all of the consumers should (theoretically) be done.
	ks.consumer.Close()
}

func newKafkaStream(clientID string, zk ZookeeperClient) (kafkaStream, error) {
	brokers, err := findRegisteredBrokers(zk)

	if err != nil {
		return nil, err
	}

	client, err := sarama.NewClient(brokers, getConfig(clientID))

	if err != nil {
		return nil, fmt.Errorf("Failed to instantiate new client. %v", err)
	}

	// Now that we have a client, let's start a consumer up.
	consumer, err := sarama.NewConsumerFromClient(client)

	if err != nil {
		client.Close()
		return nil, fmt.Errorf("Failed to open new consumer from client. %v", err)
	}

	stream := new(kafkaStreamImpl)
	stream.client = client
	stream.consumer = consumer
	stream.consumers = make(map[*streamConsumer]bool)

	return stream, nil
}
