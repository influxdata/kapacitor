package storage

import (
	"bytes"

	"github.com/boltdb/bolt"
)

// Bolt implementation of Store
type Bolt struct {
	db     *bolt.DB
	bucket []byte
}

func NewBolt(db *bolt.DB, bucket string) *Bolt {
	return &Bolt{
		db:     db,
		bucket: []byte(bucket),
	}
}

func (b *Bolt) View(f func(tx ReadOnlyTx) error) error {
	return DoView(b, f)
}

func (b *Bolt) Update(f func(tx Tx) error) error {
	return DoUpdate(b, f)
}

func (b *Bolt) put(tx *bolt.Tx, key string, value []byte) error {
	bucket, err := tx.CreateBucketIfNotExists(b.bucket)
	if err != nil {
		return err
	}
	err = bucket.Put([]byte(key), value)
	if err != nil {
		return err
	}
	return nil
}

func (b *Bolt) Put(key string, value []byte) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		return b.put(tx, key, value)
	})
}

func (b *Bolt) get(tx *bolt.Tx, key string) (*KeyValue, error) {
	bucket := tx.Bucket(b.bucket)
	if bucket == nil {
		return nil, ErrNoKeyExists
	}

	val := bucket.Get([]byte(key))
	if val == nil {
		return nil, ErrNoKeyExists
	}
	value := make([]byte, len(val))
	copy(value, val)
	return &KeyValue{
		Key:   key,
		Value: value,
	}, nil
}

func (b *Bolt) Get(key string) (kv *KeyValue, err error) {
	err = b.db.View(func(tx *bolt.Tx) error {
		kv, err = b.get(tx, key)
		return err
	})
	return
}

func (b *Bolt) delete(tx *bolt.Tx, key string) error {
	bucket := tx.Bucket(b.bucket)
	if bucket == nil {
		return nil
	}
	return bucket.Delete([]byte(key))
}

func (b *Bolt) Delete(key string) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		return b.delete(tx, key)
	})
}

func (b *Bolt) exists(tx *bolt.Tx, key string) (bool, error) {
	bucket := tx.Bucket(b.bucket)
	if bucket == nil {
		return false, nil
	}
	val := bucket.Get([]byte(key))
	exists := val != nil
	return exists, nil
}

func (b *Bolt) Exists(key string) (exists bool, err error) {
	err = b.db.View(func(tx *bolt.Tx) error {
		exists, err = b.exists(tx, key)
		return err
	})
	return
}

func (b *Bolt) list(tx *bolt.Tx, prefixStr string) (kvs []*KeyValue, err error) {
	bucket := tx.Bucket(b.bucket)
	if bucket == nil {
		return
	}

	cursor := bucket.Cursor()
	prefix := []byte(prefixStr)

	for key, v := cursor.Seek(prefix); bytes.HasPrefix(key, prefix); key, v = cursor.Next() {
		value := make([]byte, len(v))
		copy(value, v)

		kvs = append(kvs, &KeyValue{
			Key:   string(key),
			Value: value,
		})
	}
	return
}

func (b *Bolt) List(prefix string) (kvs []*KeyValue, err error) {
	err = b.db.View(func(tx *bolt.Tx) error {
		kvs, err = b.list(tx, prefix)
		return err
	})
	return
}

func (b *Bolt) BeginTx() (Tx, error) {
	return b.newTx(true)
}

func (b *Bolt) BeginReadOnlyTx() (ReadOnlyTx, error) {
	return b.newTx(false)
}

func (b *Bolt) newTx(write bool) (*boltTx, error) {
	tx, err := b.db.Begin(write)
	if err != nil {
		return nil, err
	}
	return &boltTx{
		b:  b,
		tx: tx,
	}, nil
}

// BoltTx wraps an underlying bolt.Tx type to implement the Tx interface.
type boltTx struct {
	b  *Bolt
	tx *bolt.Tx
}

func (t *boltTx) Get(key string) (*KeyValue, error) {
	return t.b.get(t.tx, key)
}

func (t *boltTx) Exists(key string) (bool, error) {
	return t.b.exists(t.tx, key)
}

func (t *boltTx) List(prefix string) ([]*KeyValue, error) {
	return t.b.list(t.tx, prefix)
}

func (t *boltTx) Put(key string, value []byte) error {
	return t.b.put(t.tx, key, value)
}

func (t *boltTx) Delete(key string) error {
	return t.b.delete(t.tx, key)
}

func (t *boltTx) Commit() error {
	return t.tx.Commit()
}

func (t *boltTx) Rollback() error {
	return t.tx.Rollback()
}
