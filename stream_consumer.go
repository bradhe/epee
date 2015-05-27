package epee

import (
	"github.com/Shopify/sarama"
	"log"
	"sync"
)

type StreamConsumer struct {
	// Indicates whether or not we should kill this.
	closing bool

	// The wait group to use when creating the consumer.
	wg sync.WaitGroup

	// The partition consumer that is backing this whoooole thing.
	partitionConsumer sarama.PartitionConsumer

	// The channel to deliver messages to
	dst chan Message
}

func (sc *StreamConsumer) run() {
	// Always make sure these guys get taken care of when we exit this function.
	defer func() {
		close(sc.dst)
		sc.partitionConsumer.Close()
		sc.wg.Done()
	}()

	messages := sc.partitionConsumer.Messages()
	errors := sc.partitionConsumer.Errors()

	for {
		if sc.closing {
			break
		}

		select {
		case message := <-messages:
			// We'll instantiate one of our own messages and keep it 'round incase we
			// want to keep goin'.
			sc.dst <- Message{
				Offset: message.Offset,
				Value:  message.Value,
				Topic:  message.Topic,
			}
		case err := <-errors:
			if err.Err == sarama.ErrClosedClient {
				log.Printf("ERROR: Kafka connection is closed. Stopping consumer.", err.Err)
				break
			} else {
				log.Printf("ERROR: Issue pulling from Kafka. %v\n", err.Err)
			}
		}
	}
}

func (sc *StreamConsumer) Messages() <-chan Message {
	return sc.dst
}

func (sc *StreamConsumer) Start() {
	sc.wg.Add(1)
	go sc.run()
}

func (sc *StreamConsumer) Close() {
	// Signal that it's time for this guy to stop.
	sc.closing = true

	// Wait for it to stop now...
	sc.wg.Wait()
}

func NewStreamConsumer(ch chan Message, partitionConsumer sarama.PartitionConsumer) *StreamConsumer {
	sc := new(StreamConsumer)
	sc.dst = ch
	sc.partitionConsumer = partitionConsumer
	return sc
}
