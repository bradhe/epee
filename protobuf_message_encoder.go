package epee

import (
	"github.com/golang/protobuf/proto"
	"sync"
)

type protobufMessageEncoder struct {
	sync.Mutex

	message proto.Message

	bytes []byte
}

func (p *protobufMessageEncoder) encodeIfNeeded() error {
	var err error

	p.Lock()
	if len(p.bytes) == 0 && p.message != nil {
		p.bytes, err = proto.Marshal(p.message)
	}
	p.Unlock()

	return err
}

func (p *protobufMessageEncoder) Encode() ([]byte, error) {
	err := p.encodeIfNeeded()

	if err != nil {
		return []byte{}, err
	}

	return p.bytes, err
}

func (p *protobufMessageEncoder) Length() int {
	err := p.encodeIfNeeded()

	if err != nil {
		return 0
	}

	return len(p.bytes)
}

func newProtobufMessageEncoder(message proto.Message) *protobufMessageEncoder {
	encoder := new(protobufMessageEncoder)
	encoder.message = message
	return encoder
}
