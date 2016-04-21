package storage

import "errors"

// Common errors that can be returned
var (
	ErrNoKeyExists = errors.New("no key exists")
)

// Common interface for interacting with a simple Key/Value storage
type Interface interface {
	// Store a value.
	Put(key string, value []byte) error
	// Retrieve a value.
	Get(key string) (*KeyValue, error)
	// Delete a key.
	// Deleting a non-existent key is not an error.
	Delete(key string) error
	// Check if a key exists>
	Exists(key string) (bool, error)
	// List all values with given prefix.
	List(prefix string) ([]*KeyValue, error)
}

type KeyValue struct {
	Key   string
	Value []byte
}

// Return a list of values from a list of KeyValues using an offset/limit bound and a match function.
func DoListFunc(list []*KeyValue, match func(value []byte) bool, offset, limit int) [][]byte {
	l := len(list)
	upper := offset + limit
	if upper > l {
		upper = l
	}
	size := upper - offset
	if size <= 0 {
		// No more results
		return nil
	}
	matches := make([][]byte, 0, size)
	i := 0
	for _, kv := range list {
		if !match(kv.Value) {
			continue
		}
		// Count matched
		i++

		// Skip till offset
		if i <= offset {
			continue
		}

		matches = append(matches, kv.Value)

		// Stop once limit reached
		if len(matches) == size {
			break
		}
	}
	return matches
}
