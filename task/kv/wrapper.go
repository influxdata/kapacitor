package kv

import "github.com/influxdata/kapacitor/services/storage"

// BoltTx wraps an underlying bolt.Tx type to implement the Tx interface.
type wrappedTx struct {
	tx     storage.Tx
	prefix string
}

func (t *wrappedTx) Get(key string) (*storage.KeyValue, error) {
	return t.tx.Get(t.prefix + "/" + key)
}

func (t *wrappedTx) Exists(key string) (bool, error) {
	return t.tx.Exists(t.prefix + "/" + key)
}

func (t *wrappedTx) List(prefix string) ([]*storage.KeyValue, error) {
	return t.tx.List(t.prefix + "/" + prefix)
}

func (t *wrappedTx) Put(key string, value []byte) error {
	return t.tx.Put(t.prefix+"/"+key, value)
}

func (t *wrappedTx) Delete(key string) error {
	return t.tx.Delete(t.prefix + "/" + key)
}

// BoltTx wraps an underlying bolt.Tx type to implement the Tx interface.
type wrappedReadTx struct {
	tx     storage.ReadOnlyTx
	prefix string
}

func (t *wrappedReadTx) Get(key string) (*storage.KeyValue, error) {
	return t.tx.Get(t.prefix + "/" + key)
}

func (t *wrappedReadTx) Exists(key string) (bool, error) {
	return t.tx.Exists(t.prefix + "/" + key)
}

func (t *wrappedReadTx) List(prefix string) ([]*storage.KeyValue, error) {
	return t.tx.List(t.prefix + "/" + prefix)
}
