package epee

import (
	"encoding/json"
	"log"
	"path"
)

type MockZookeeperClient struct {
	paths map[string][]byte
}

func (zk *MockZookeeperClient) List(prefix string) ([]string, error) {
	if prefix == "" {
		return []string{}, nil
	}

	// If the prefix ends with a / we'll remove it.
	if prefix[len(prefix)-1] == '/' {
		prefix = prefix[0 : len(prefix)-1]
	}

	paths := make([]string, 0)

	for k := range zk.paths {
		dir := path.Dir(k)

		if prefix == dir {
			paths = append(paths, k)
		}
	}

	return paths, nil
}

func (zk *MockZookeeperClient) Get(path string, i interface{}) error {
	bytes, ok := zk.paths[path]

	if !ok {
		return ErrNotFound
	}

	if ok {
		return json.Unmarshal(bytes, i)
	}

	// Not found.
	return nil
}

func (zk *MockZookeeperClient) Set(path string, i interface{}) error {
	log.Printf("ZK: Setting %s to %v", path, i)
	bytes, err := json.Marshal(i)

	if err == nil {
		zk.paths[path] = bytes
	}

	return err
}

func NewMockZookeeperClient() ZookeeperClient {
	zk := new(MockZookeeperClient)
	zk.paths = make(map[string][]byte)
	return zk
}
