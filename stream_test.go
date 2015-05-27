package epee

import (
	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	"path"
	"sync"
	"testing"
)

func TestSyncsStreamPositionOnSuccessfulFlush(t *testing.T) {
	testMessage := TestMessage{
		Name: proto.String("My Test"),
	}
	bytes, err := proto.Marshal(&testMessage)

	if err != nil {
		t.Fatalf("Expected no error marshalling test message, got %v", err)
	}

	zk := newMockZookeeperClient()
	ks, consumer := newMockKafkaStream(t, "test-client-1", zk)

	// Set up expectations for Kafka.
	pc := consumer.ExpectConsumePartition("topic-1", 1, sarama.OffsetOldest)
	pc.YieldMessage(&sarama.ConsumerMessage{
		Key:       []byte("test"),
		Value:     bytes,
		Topic:     "topic-1",
		Partition: int32(1),
		Offset:    int64(1),
	})

	stream, err := NewStreamFromKafkaStream("test-client-1", zk, ks)

	if err != nil {
		t.Errorf("Failed to instantiate stream. Got %v", err)
	}

	var wg sync.WaitGroup
	proc := testProcessor{&testMessage, t, &wg}
	wg.Add(1)

	stream.Register("topic-1", TestMessage{})
	err = stream.Stream("topic-1", 1, &proc)

	// Stream should have started successfully.
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Wait for everything to finish up
	wg.Wait()

	// Now let's try to flush. Should be dirty.
	stream.FlushAll()

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	var offset int64
	err = zk.Get(path.Join(DefaultZookeeperPrefix, "test-client-1", "topic-1", "1"), &offset)

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if offset != 1 {
		t.Fatalf("Expected saved offset to be 1, got %d", offset)
	}
}

func TestPicksUpFromLastFlush(t *testing.T) {
	// This is the path that we'll expect data to come from.
	fullPath := path.Join(DefaultZookeeperPrefix, "test-client-1", "topic-1", "1")

	// Our example test message.
	testMessage := TestMessage{
		Name: proto.String("My Test"),
	}
	bytes, err := proto.Marshal(&testMessage)

	if err != nil {
		t.Fatalf("Expected no error marshalling test message, got %v", err)
	}

	zk := newMockZookeeperClient()
	zk.Set(fullPath, int64(123456))

	ks, consumer := newMockKafkaStream(t, "test-client-1", zk)

	// Set up expectations in kafak. Big thing here is that the offset should be
	// 1 after the offset stored in ZK.
	pc := consumer.ExpectConsumePartition("topic-1", 1, 123457)
	pc.YieldMessage(&sarama.ConsumerMessage{
		Key:       []byte("test"),
		Value:     bytes,
		Topic:     "topic-1",
		Partition: int32(1),
		Offset:    int64(123456),
	})

	// Okay now we can try all this stuff.
	stream, err := NewStreamFromKafkaStream("test-client-1", zk, ks)

	if err != nil {
		t.Errorf("Failed to instantiate stream. Got %v", err)
	}

	var wg sync.WaitGroup
	proc := testProcessor{&testMessage, t, &wg}
	wg.Add(1)

	stream.Register("topic-1", TestMessage{})
	err = stream.Stream("topic-1", 1, &proc)

	// Stream should have started successfully.
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Wait for everything to finish up
	wg.Wait()

	// NOTE: This test will fail due to expectations set on the Sarama mock if
	// the wrong offset is used or something like that.
}

type testProcessor struct {
	original *TestMessage

	t *testing.T

	wg *sync.WaitGroup
}

func (t *testProcessor) Process(offset int64, message proto.Message) error {
	defer t.wg.Done()

	m, ok := message.(*TestMessage)

	if !ok {
		t.t.Fatalf("Expected message type TestMessage, got %v", message)
	}

	if t.original.GetName() != m.GetName() {
		t.t.Fatalf("Expected message name '%s', got '%s'", t.original.GetName(), m.GetName())
	}

	return nil
}

func (t *testProcessor) Flush() error {
	// noop
	return nil
}
