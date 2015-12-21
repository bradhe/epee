package epee

import (
	"github.com/codegangsta/cli"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"sync"
)

var (
	// Used to control concurrent access to the stream processor collection.
	pmut sync.RWMutex

	// All the registered stream processors. Only the ones that are requested by
	// the user will actualy be started.
	streamProcessors map[string][]StreamProcessor

	// Used to control concurrent access to the stream types collection.
	tmut sync.RWMutex

	// A mapping of types to topics. All types must ultimately be marshalable
	// from proto.Message.
	streamTypes map[string]reflect.Type

	// Logger conditionally setup when the service starts.
	Logger *log.Logger
)

func init() {
	streamProcessors = make(map[string][]StreamProcessor)
	streamTypes = make(map[string]reflect.Type)

	// By default we'll just log to nothing.
	Logger = log.New(ioutil.Discard, "", 0)
}

func RegisterStreamProcessor(topic string, proc StreamProcessor) {
	pmut.Lock()
	defer pmut.Unlock()

	arr, ok := streamProcessors[topic]

	if ok {
		streamProcessors[topic] = append(arr, proc)
	} else {
		streamProcessors[topic] = []StreamProcessor{proc}
	}
}

func GetStreamProcessors(topic string) []StreamProcessor {
	pmut.RLock()
	defer pmut.RUnlock()
	return streamProcessors[topic]
}

func RegisterType(topic string, t reflect.Type) {
	tmut.Lock()
	defer tmut.Unlock()

	streamTypes[topic] = t
}

func GetStreamType(topic string) (reflect.Type, bool) {
	tmut.RLock()
	defer tmut.RUnlock()

	// go can be real silly sometimes.
	t, ok := streamTypes[topic]
	return t, ok
}

func doStart(c *cli.Context) {
	// Make sure that the given topic and partition actually exist.
}

func Run(name string) {
	app := cli.NewApp()
	app.Name = name
	app.Usage = "An Epee-based stream processor."
	app.Commands = []cli.Command{
		{
			Name:   "start",
			Usage:  "create a new droplet",
			Action: doStart,
		},
	}

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "topic",
			Value: "",
			Usage: "the topic to subscribe to",
		},
		cli.StringFlag{
			Name:  "client-id",
			Value: "",
			Usage: "the client ID to use when running this consumer",
		},
		cli.IntFlag{
			Name:  "partition",
			Value: 0,
			Usage: "the partition of the topic to subscribe to",
		},
	}

	app.Run(os.Args)
}
