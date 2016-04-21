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

func (b *Bolt) Put(key string, value []byte) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(b.bucket)
		if err != nil {
			return err
		}
		err = bucket.Put([]byte(key), value)
		if err != nil {
			return err
		}
		return nil
	})
}
func (b *Bolt) Get(key string) (*KeyValue, error) {
	var value []byte
	err := b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(b.bucket)
		if bucket == nil {
			return ErrNoKeyExists
		}

		val := bucket.Get([]byte(key))
		if val == nil {
			return ErrNoKeyExists
		}
		value = make([]byte, len(val))
		copy(value, val)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &KeyValue{
		Key:   key,
		Value: value,
	}, nil
}

func (b *Bolt) Delete(key string) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(b.bucket)
		if bucket == nil {
			return nil
		}
		return bucket.Delete([]byte(key))
	})
}

func (b *Bolt) Exists(key string) (bool, error) {
	var exists bool
	err := b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(b.bucket)
		if bucket == nil {
			return nil
		}

		val := bucket.Get([]byte(key))
		exists = val != nil
		return nil
	})
	return exists, err
}

func (b *Bolt) List(prefix string) (kvs []*KeyValue, err error) {
	err = b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(b.bucket)
		if bucket == nil {
			return nil
		}

		cursor := bucket.Cursor()
		prefix := []byte(prefix)

		for key, v := cursor.Seek(prefix); bytes.HasPrefix(key, prefix); key, v = cursor.Next() {
			value := make([]byte, len(v))
			copy(value, v)

			kvs = append(kvs, &KeyValue{
				Key:   string(key),
				Value: value,
			})
		}
		return nil
	})
	return kvs, err
}
