package epee

// Wraps a message from the stream. This is the internal type used in place of
// Sarama.
type Message struct {
	// The offset that this message appears from within Kafka.
	Offset int64

	// The topic that this message originated from.
	Topic string

	// Raw message payload.
	Value []byte
}
