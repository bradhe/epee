package epee

import (
	"github.com/golang/protobuf/proto"
)

type StreamProcessor interface {
	// Deserializes and publishes a message.
	Process(offset int64, message proto.Message) error

	// Called after a certain number of ticks/intervals.
	Flush() error
}
