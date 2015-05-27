package epee

import (
	"encoding/json"
	"errors"
	"github.com/samuel/go-zookeeper/zk"
	"log"
	"math"
	"path"
	"strconv"
	"strings"
	"time"
)

var (
	// This is just blindly passed in to the ZK client.
	DefaultSessionTimeout = 1 * time.Second

	ErrNotFound = errors.New("not found")
)

type ZookeeperClient interface {
	Get(path string, i interface{}) error
	Set(path string, i interface{}) error
	List(loc string) ([]string, error)
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
		log.Printf("WARNING: Failed to parse sequence in %v (%s)", str, str[i:])
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

	// We need to deserialize this value.
	err = json.Unmarshal(bytes, i)

	if err != nil {
		return err
	}

	return nil
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
