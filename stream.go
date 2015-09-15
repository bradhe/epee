package epee

import (
	"fmt"
	"github.com/bradhe/nbonce"
	"github.com/golang/protobuf/proto"
	"log"
	"path"
	"reflect"
	"sync"
	"time"
)

const (
	// Number of seconds to wait between flush checks.
	DefaultMonitorTimeout = 5 * time.Second
)

var (
	// Max size of the messages producers and consumers try to fetch.
	MaxMessageSize = 11000000
)

func offsetPath(clientID string, topic string, partition int) string {
	return path.Join(DefaultZookeeperPrefix, clientID, topic, fmt.Sprintf("%d", partition))
}

func keyPath(clientID string, key string) string {
	return path.Join(DefaultZookeeperPrefix, clientID, key)
}

type Stream struct {
	sync.Mutex

	// Types to be instantiated and passted in to the handlers.
	types map[string]reflect.Type

	// The zookeeper cluster our service is connecting to.
	zk ZookeeperClient

	// A stream of events in Kafka
	ks kafkaStream

	// The ID of the client. We'll use this for storing data elsewhere.
	clientID string

	// All the consumers that were created during this stream's lifecycle.
	consumers map[string]*streamConsumer

	// All the proxies created during this stream's lifecycle.
	proxies map[string]*streamProcessorProxy

	// Used to start flushes.
	trigger nbonce.NonblockingOnce
}

func (q *Stream) dispatch(proc StreamProcessor, t reflect.Type, message *Message) error {
	obj, ok := reflect.New(t).Interface().(proto.Message)

	if !ok {
		log.Printf("Failed to cast type %v to to message.", t)
		return ErrDecodingMessageFailed
	}

	err := proto.Unmarshal(message.Value, obj)

	if err != nil {
		log.Printf("Failed to unmarshal object: %v", err)
		return ErrDecodingMessageFailed
	}

	return proc.Process(message.Offset, obj)
}

func (q *Stream) runConsumer(topic string, partition int, src <-chan *Message, proc StreamProcessor) {
	for message := range src {
		t, ok := q.types[message.Topic]

		if !ok {
			// TODO: Should we actually panic here? Or should we do something else?
			log.Panicf("Failed to find registerd type for topic %s", message.Topic)
		}

		err := q.dispatch(proc, t, message)

		if err != nil {
			log.Printf("ERROR: Failed to process message on topic [%s, %d]. %v", topic, partition, err)
		}

		// Now that we have a stream, we want to wait some time and start a flush.
		q.trigger.Do(func() {
			time.Sleep(DefaultMonitorTimeout)
			q.flushAll()
		})
	}
}

func (q *Stream) startConsumer(topic string, partition int, proc StreamProcessor) error {
	// Let's get our consumer's existing offset.
	var offset int64
	err := q.zk.Get(offsetPath(q.clientID, topic, partition), &offset)

	// If the error was ErrNotFound then we can rely on offset to be 0.
	if err != nil && err != ErrNotFound {
		return err
	} else if err == ErrNotFound {
		// Start at the beginning!
		offset = 0
	} else {
		// We'll add 1 to the offset, so we don't re-process a tuple.
		offset += 1
	}

	// Now let's see if we can start a consumer from the specified offset.
	consumer, err := q.ks.Consume(topic, partition, offset)

	if err != nil {
		return err
	}

	// Let's see if we already have a proxy here.
	key := fmt.Sprintf("%s/%d", topic, partition)
	old, ok := q.consumers[fmt.Sprintf(key)]

	if ok {
		// there is already a proxy here! Let's kill this one and start over again.
		q.ks.CancelConsumer(old)
	}

	// We'll start monitoring this stream processor to figure out when it needs
	// to be flushed.
	q.consumers[key] = consumer

	// Everything passed so we can start the consumer up now!
	go q.runConsumer(topic, partition, consumer.Messages(), proc)
	return nil
}

// Registers the type that a topic should deserialize it's content to. It's
// assumed that the type is a proto.Message. Every new event in the stream on
// the topic topic will be deserialized with this type.
func (q *Stream) Register(topic string, obj interface{}) {
	t := reflect.TypeOf(obj)
	q.types[topic] = t
}

func (q *Stream) Stream(topic string, partition int, proc StreamProcessor) error {
	q.Lock()
	defer q.Unlock()

	proxy := newStreamProcessorProxy(proc)
	err := q.startConsumer(topic, partition, proxy)

	if err != nil {
		log.Printf("ERROR: Failed to start consumer %s for %s:%d. %v", q.clientID, topic, partition, err)
	} else {
		// Let's monitor this proxy for any changes, then we'll schedule it for
		// flushing.
		key := fmt.Sprintf("%s/%d", topic, partition)
		q.proxies[key] = proxy
	}

	return err
}

func (q *Stream) flushAll() {
	for key, proxy := range q.proxies {
		if proxy.Dirty() {
			err := proxy.Flush()

			if err != nil {
				// Flush failed! Who to tell??
				log.Printf("ERROR: Flushing failed. %v", err)
			} else {
				// This is kind of hacky...but it generates the correct path based on
				// the key in the proxies hash. Le sigh.
				log.Printf("INFO: Flushing successful. Setting %s to %d", keyPath(q.clientID, key), proxy.LastOffset())
				q.zk.Set(keyPath(q.clientID, key), proxy.LastOffset())
			}
		}
	}
}

func (q *Stream) Wait() {
	var wg sync.WaitGroup
	wg.Add(1)
	wg.Wait()
}

func NewStream(clientID string, zk ZookeeperClient) (*Stream, error) {
	ks, err := newKafkaStream(clientID, zk)

	if err != nil {
		return nil, err
	}

	stream := new(Stream)
	stream.types = make(map[string]reflect.Type)
	stream.zk = zk
	stream.ks = ks
	stream.clientID = clientID

	return stream, nil
}

func MustGetStream(clientID string, zk ZookeeperClient) *Stream {
	var ks kafkaStream

	for {
		var err error

		ks, err = newKafkaStream(clientID, zk)

		// If this is a restartable error, let's get this shit rollin'.
		if err == ErrNoBrokers {
			<-time.After(3 * time.Second)
		} else if err != nil {
			panic(err)
		} else {
			break
		}
	}

	// NOTE: We can safely ignore this because we're bad asses.
	stream, _ := newStreamWithKafkaStream(clientID, zk, ks)
	return stream
}

func newStreamWithKafkaStream(clientID string, zk ZookeeperClient, ks kafkaStream) (*Stream, error) {
	stream := new(Stream)
	stream.zk = zk
	stream.ks = ks
	stream.clientID = clientID

	stream.types = make(map[string]reflect.Type)
	stream.proxies = make(map[string]*streamProcessorProxy)
	stream.consumers = make(map[string]*streamConsumer)

	// We want to be able to reschedule this goroutine if it hasn't already been
	// scheduled.
	stream.trigger.Resettable = true

	return stream, nil
}
