package epee

import (
	"github.com/Shopify/sarama"
	"testing"
)

func TestMockKafkaStreamsReturnDataFromChannel(t *testing.T) {
	zk := newMockZookeeperClient()
	kafka, consumer := newMockKafkaStream(t, "client-1", zk)
	pc := consumer.ExpectConsumePartition("my-topic", 1, sarama.OffsetOldest)

	pc.YieldMessage(&sarama.ConsumerMessage{
		Key:       []byte("test"),
		Value:     []byte("test"),
		Topic:     "my-topic",
		Partition: int32(1),
		Offset:    int64(1),
	})

	c, err := kafka.Consume("my-topic", 1, 0)

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	message := <-c.Messages()

	if string(message.Value) != "test" {
		t.Fatalf("Expected value 'test', got %s", message.Value)
	}
}
