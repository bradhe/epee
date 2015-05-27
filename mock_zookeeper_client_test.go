package epee

import (
	"testing"
)

func contains(strs []string, str string) bool {
	for _, s := range strs {
		if s == str {
			return true
		}
	}

	return false
}

func TestSetAndGetReturnsPrimitivesCorrectly(t *testing.T) {
	mock := NewMockZookeeperClient()
	err := mock.Set("/hello/world", 1)

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	var res int
	err = mock.Get("/hello/world", &res)

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if res != 1 {
		t.Fatalf("Expected 1, got %d", res)
	}
}

func TestSetAndGetReturnsTypesCorrectly(t *testing.T) {
	mock := NewMockZookeeperClient()
	err := mock.Set("/hello/world", "Hello, World!")

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	var res string
	err = mock.Get("/hello/world", &res)

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if res != "Hello, World!" {
		t.Fatalf("Expected 'Hello, World!', got %s", res)
	}
}

func TestListReturnsPathsToChildren(t *testing.T) {
	mock := NewMockZookeeperClient()
	err := mock.Set("/hello/mother", 1)

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	err = mock.Set("/hello/father", 2)

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	list, err := mock.List("/hello")

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if len(list) != 2 {
		t.Fatalf("Expected 2 elements, got %d", len(list))
	}

	if !contains(list, "/hello/mother") {
		t.Fatalf("Expected %v to contain /hello/mother", list)
	}

	if !contains(list, "/hello/father") {
		t.Fatalf("Expected %v to contain /hello/father", list)
	}
}

func TestListReturnsPathsToChildrenWhenThereIsASuffixSlash(t *testing.T) {
	mock := NewMockZookeeperClient()
	err := mock.Set("/hello/mother", 1)

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	err = mock.Set("/hello/father", 2)

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Same as above, but with a suffix.
	list, err := mock.List("/hello/")

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if len(list) != 2 {
		t.Fatalf("Expected 2 elements, got %d", len(list))
	}

	if !contains(list, "/hello/mother") {
		t.Fatalf("Expected %v to contain /hello/mother", list)
	}

	if !contains(list, "/hello/father") {
		t.Fatalf("Expected %v to contain /hello/father", list)
	}
}
