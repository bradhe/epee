package epee

import (
	"github.com/Shopify/sarama"
	"sync"
)

type streamConsumer struct {
	// Indicates whether or not we should kill this.
	closing bool

	// The wait group to use when creating the consumer.
	wg sync.WaitGroup

	// The partition consumer that is backing this whoooole thing.
	partitionConsumer sarama.PartitionConsumer

	// The channel to deliver messages to
	dst chan *Message
}

func (sc *streamConsumer) run() {
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
			sc.dst <- &Message{
				Offset: message.Offset,
				Value:  message.Value,
				Topic:  message.Topic,
			}
		case err := <-errors:
			if err.Err == sarama.ErrClosedClient {
				logError("Kafka connection is closed. Stopping consumer. %v", err.Err)
				break
			} else {
				logError("Failed to get errors from Sarama. %v", err.Err)
			}
		}
	}
}

func (sc *streamConsumer) Messages() <-chan *Message {
	return sc.dst
}

func (sc *streamConsumer) Start() {
	sc.wg.Add(1)
	sc.run()
}

func (sc *streamConsumer) Close() {
	// Signal that it's time for this guy to stop.
	sc.closing = true

	// Wait for it to stop now...
	sc.wg.Wait()
}

func newStreamConsumer(ch chan *Message, partitionConsumer sarama.PartitionConsumer) *streamConsumer {
	sc := new(streamConsumer)
	sc.dst = ch
	sc.partitionConsumer = partitionConsumer
	return sc
}
