package epee

import (
	"github.com/golang/protobuf/proto"
	"sync"
)

type StreamProcessorProxy struct {
	sync.Mutex

	// Indicates if we need to flush this processor or not.
	dirty bool

	// Last known offset that was successfully processed.
	lastOffset int64

	// the underlying processor.
	proc StreamProcessor
}

func (spp *StreamProcessorProxy) Process(offset int64, message proto.Message) error {
	spp.Lock()
	defer spp.Unlock()

	err := spp.proc.Process(offset, message)

	if err == nil {
		spp.lastOffset = offset
		spp.dirty = true
	}

	return err
}

func (spp *StreamProcessorProxy) LastOffset() int64 {
	return spp.lastOffset
}

func (spp *StreamProcessorProxy) Flush() error {
	spp.Lock()
	defer spp.Unlock()

	err := spp.proc.Flush()

	if err == nil {
		// This is no longer dirty!
		spp.dirty = false
	}

	return err
}

func (spp *StreamProcessorProxy) Dirty() bool {
	spp.Lock()
	defer spp.Unlock()

	return spp.dirty
}

func NewStreamProcessorProxy(proc StreamProcessor) *StreamProcessorProxy {
	spp := new(StreamProcessorProxy)
	spp.proc = proc
	return spp
}
