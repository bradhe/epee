package epee

type Message struct {
	Offset int64

	Topic string

	Value []byte
}
