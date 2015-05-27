Epee
===

A (very) simple golang stream processing library built on top of Kafka and
protobuf. It is assumed that only one message type lives in a given Kafka
topic.

Each stream processor gets a unique client ID per topic/partition and only one
processor at a time can online. Coordination between processors is handled by
Zookeeper.

Stream processor state is managed by the processor itself: Periodically the
`Flush` function will be called and the processor is expected to flush it's
current state. The state should be reloaded by the processor when it comes back
online.

## Example

Here's a very simple example of how to use Epee. First we need to assume that
you have a protobuf definition for your messages. For this example we'll use
the following.

```protobuf
package main;

message MyCounter {
  required int64 Count = 1;
}
```

The steps for consuming these messages on a Kafka topic are basically as
follows.

1. Instantiate a new `epee.Stream` object, which represents a connection to Kafka.
1. Register the `MyCounter` type with the stream object for a given topic so
	 the stream knows how to deserialize the messages.
1. Implement the `epee.StreamProcessor` interface in your own type.
1. Tell your stream object to start streaming to your new type.

Here's the basic example code. You can also [view it in the example
repo](https://github.com/bradhe/epee-example) if you want.

```golang
package main

import (
  "flag"
  "fmt"
  "github.com/golang/protobuf/proto"
  "github.com/bradhe/epee"
)

const (
  // This is the topic that the stream processor will listen to.
  TopicName = "my-topic"

  // This is the partition that the processor will listen on.
  Partition = 1

  // NOTE: This must be unique to the topic and partition the stream processor is 
  // going to consume.
  DefaultClientID = "my-client-1"
)

var (
  stream *epee.Stream
  
  // Parameterize where Zookeeper lives.
  ZookeeperHost = flag.String("zookeeper-host", "localhost:2181", "zookeeper host")
)

// This type encapsulates the stream processor and will implement the
// epee.StreamProcessor interface.
type MyStreamProcessor struct {
  Total int64
}

// The process method is called once for each message in the queue. If the message
// is successfully processed the related offset will be marked as "processed" so
// that when clients resume later this message doesn't get re-processed.
func (sp *MyStreamProcessor) Process(offset int64, message proto.Message) error {
  counter, ok := message.(*MyCounter)

  if !ok {
    return fmt.Errorf("failed to convert message to application-native type")	
  }

  sp.Total += counter.GetCount()
  return nil
}

// The flush method will be periodically called (once every 10 seconds by
// default). This method is used to flush the processor's state so the jobs can
// be resumed if something goes wrong.
func (sp *MyStreamProcessor) Flush() error {
  // TODO: Flush the total to something here.
  return nil
}

func init() {
  var err error

  zk, err := epee.NewZookeeperClient(*MyZookeeperServer)

  if err != nil {
    panic(err)
  }
  
  // Assuming your Kafka brokers are registered in Zookeeper...
  stream, err = epee.NewStreamFromZookeeper(DefaultClientID, zk)

  if err != nil {
    panic(err)
  }

  // This tells the stream how to deserialize the message in Kafka.
  stream.Register(TopicName, &MyCounter{})
}

func main() {
  stream.Stream(TopicName, Partition, &MyStreamProcessor{})

	// The stream processor is now running in a goroutine in the background. The
  // main thread can continue doing whatever, or we can just sit here and wait.
  stream.Wait()
}
```

## Development

A `Makefile` has been included which has targets for setting up a development
environment and all that jazz. Getting up and running is pretty simple.

```bash
$ git clone git@github.com:bradhe/epee.git $GOPATH/src/github.com/bradhe/epee
# ...
$ cd $GOPATH/src/github.com/bradhe/epee
$ make setup
# * Downloads/installs protoc if it is missing.
# * Gets relevant dependencies
$ make test
```

If you have something you'd like to contribute, just open a pull request!

## History

* v0.1.0 - 2015-05-27 - Initial release
