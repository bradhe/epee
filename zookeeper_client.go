package epee

import (
	"encoding/json"
	"errors"
	"github.com/samuel/go-zookeeper/zk"
	"math"
	"path"
	"strconv"
	"strings"
	"time"
)

var (
	DefaultZookeeperPrefix = "/epee"

	// This is just blindly passed in to the ZK client.
	DefaultSessionTimeout = 5 * time.Second
)

var (
	ErrZookeeperNodeExists = errors.New("node exists")
)

// Wraps common Zookeeper operations behind an interface to make it easier to
// deal with. Epee also provides a default implementation of ZookeeperClient to
// make your life even easier.
type ZookeeperClient interface {
	// Gets the JSON-encoded value at the path specified with
	// DefaultZookeeperPrefix prepended to the path.
	Get(path string, i interface{}) error

	// JSON encodes i and writes that value to the path specified with
	// DefaultZookeeperPrefix prepended to it.
	Set(path string, i interface{}) error

	// List all of the child paths under loc. This is considered an absolute path
	// (e.g. DefaultZookeeperPrefix is not prepended).
	List(loc string) ([]string, error)

	// Try to acquire a lock by name. If the lock is already acquired by another
	// client, acquired will be false. If an error occurse, err will be non-nil
	// and acquired will be false.
	TryLock(name string) (acquired bool, err error)

	// Close the connection to zookeeper.
	Close() error
}

func split(str string) (string, uint64) {
	res := uint64(math.MaxUint64)
	i := strings.LastIndex(str, "-")

	if i == -1 {
		return str, res
	}

	prefix := str[0:i]
	seq, err := strconv.ParseInt(str[i+1:], 10, 64)

	if err != nil {
		logWarning("Failed to parse sequence in %v (%s)", str, str[i:])
	} else {
		res = uint64(seq)
	}

	return prefix, res
}

type zookeeperClientImpl struct {
	conn   *zk.Conn
	events <-chan zk.Event
}

func (c *zookeeperClientImpl) runGeneralEventLoop() {
	for _ = range c.events {
		// TODO: Anything here?
	}
}

func (r *zookeeperClientImpl) exists(loc string) bool {
	var res bool
	res, _, _ = r.conn.Exists(loc)
	return res
}

func (r *zookeeperClientImpl) mkdirs(loc string) {
	if r.exists(loc) {
		return
	}

	// Check the parent to make sure it exists...
	dir := path.Dir(loc)

	if !r.exists(dir) {
		r.mkdirs(dir)
	}

	r.conn.Create(loc, []byte{}, 0, zk.WorldACL(zk.PermAll))
}

func (c *zookeeperClientImpl) Get(loc string, i interface{}) error {
	bytes, _, err := c.conn.Get(loc)

	if err != nil {
		if err == zk.ErrNoNode {
			return ErrNotFound
		}

		return err
	}

	if len(bytes) > 0 {
		// We need to deserialize this value.
		err = json.Unmarshal(bytes, i)

		if err != nil {
			return err
		}
	}

	return nil
}

func (c *zookeeperClientImpl) Create(loc string, obj interface{}) (err error) {
	var b []byte
	b, err = json.Marshal(obj)

	if err != nil {
		return
	}

	c.mkdirs(path.Dir(loc))
	_, err = c.conn.Create(loc, b, 0, zk.WorldACL(zk.PermAll))

	// Translate this in to a friendlier message for our users.
	if err == zk.ErrNodeExists {
		err = ErrZookeeperNodeExists
	}

	return
}

func (c *zookeeperClientImpl) Set(loc string, obj interface{}) (err error) {
	var b []byte
	b, err = json.Marshal(obj)

	if err != nil {
		return
	}

	if c.exists(loc) {
		_, err = c.conn.Set(loc, b, -1)
	} else {
		c.mkdirs(path.Dir(loc))
		_, err = c.conn.Create(loc, b, 0, zk.WorldACL(zk.PermAll))
	}

	return
}

func (c *zookeeperClientImpl) List(loc string) ([]string, error) {
	paths, _, err := c.conn.Children(loc)

	if err != nil {
		return []string{}, err
	}

	fullPaths := make([]string, len(paths))

	for i := range paths {
		fullPaths[i] = path.Join(loc, paths[i])
	}

	return fullPaths, nil
}

func (c *zookeeperClientImpl) TryLock(name string) (acquired bool, err error) {
	return
}

func (c *zookeeperClientImpl) Close() error {
	c.conn.Close()
	return nil
}

func NewZookeeperClient(servers []string) (ZookeeperClient, error) {
	var err error

	c := new(zookeeperClientImpl)
	c.conn, c.events, err = zk.Connect(servers, DefaultSessionTimeout)

	if err != nil {
		return nil, err
	}

	go c.runGeneralEventLoop()

	return c, nil
}
