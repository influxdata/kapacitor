package storage

import (
	"bytes"

	bolt "go.etcd.io/bbolt"
)

// Bolt implementation of Store
type Bolt struct {
	db     *bolt.DB
	bucket [][]byte
}

func NewBolt(db *bolt.DB, bucket ...[]byte) *Bolt {
	return &Bolt{
		db:     db,
		bucket: bucket,
	}
}

// Bucket tells the Bolt to do following actions in a bucket.  A nil bucket will return a *Bolt that is targeted to the root bucket.
func (b *Bolt) Bucket(bucket []byte) *Bolt {
	if bucket == nil {
		return &Bolt{
			db:     b.db,
			bucket: nil,
		}
	}
	return &Bolt{
		db:     b.db,
		bucket: append(b.bucket, bucket),
	}
}

// Bucket tells the Bolt to do following actions in a bucket.  A nil bucket will return a *Bolt that is targeted to the root bucket.
func (b *Bolt) Store(buckets ...[]byte) Interface {
	return &Bolt{
		db:     b.db,
		bucket: buckets,
	}
}

func (b *Bolt) View(f func(tx ReadOnlyTx) error) error {
	return DoView(b, f)
}

func (b *Bolt) Update(f func(tx Tx) error) error {
	return DoUpdate(b, f)
}

func (b *Bolt) put(tx *bolt.Tx, key string, value []byte) error {
	bucket, err := tx.CreateBucketIfNotExists(b.bucket[0])
	if err != nil {
		return err
	}
	for _, buckName := range b.bucket[1:] {
		bucket, err = bucket.CreateBucketIfNotExists(buckName)
		if err != nil {
			return err
		}
	}
	return bucket.Put([]byte(key), value)
}

func (b *Bolt) Put(key string, value []byte) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		return b.put(tx, key, value)
	})
}

func (b *Bolt) get(tx *bolt.Tx, key string) (*KeyValue, error) {
	bucket := b.bucketHelper(tx)
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

// Delete removes a key from a bolt.  If the key is a bucket, it removes that.
func (b *Bolt) delete(tx *bolt.Tx, key string) error {
	bucket := b.bucketHelper(tx)
	if bucket == nil {
		return nil
	}
	cursor := bucket.Cursor()
	if cursor == nil {
		return nil
	}
	// handling for buckets
	bkey := []byte(key)
	k, v := cursor.Seek(bkey)
	if key != string(k) {
		return nil
	}
	if v == nil {
		return bucket.DeleteBucket(bkey)
	}
	// handling for regular keys
	return bucket.Delete(bkey)

}

func (b *Bolt) Delete(key string) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		return b.delete(tx, key)
	})
}

func (b *Bolt) exists(tx *bolt.Tx, key string) (bool, error) {
	bucket := b.bucketHelper(tx)
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

func (b *Bolt) bucketHelper(tx *bolt.Tx) *bolt.Bucket {
	if len(b.bucket) == 0 {
		return tx.Cursor().Bucket() //grab root bucket
	} // get the right bucket
	bucket := tx.Bucket(b.bucket[0])
	if bucket == nil {
		return nil
	}
	for _, buckName := range b.bucket[1:] {
		bucket = bucket.Bucket(buckName)
		if bucket == nil {
			return nil
		}
	}
	return bucket
}

// cursor returns a cursor at the appropriate bucket or nil if that bucket doesn't exist
func (b *Bolt) cursor(tx *bolt.Tx) *bolt.Cursor {
	if len(b.bucket) == 0 {
		return tx.Cursor() //grab root bucket
	} // get the right bucket
	bucket := tx.Bucket(b.bucket[0])
	if bucket == nil {
		return nil
	}
	for _, buckName := range b.bucket[1:] {
		bucket = bucket.Bucket(buckName)
		if bucket == nil {
			return nil
		}
	}
	return bucket.Cursor()
}

func (b *Bolt) list(tx *bolt.Tx, prefixStr string) (kvs []*KeyValue, err error) {
	cursor := b.cursor(tx)
	if cursor == nil {
		return nil, nil // no objects returned
	}

	prefix := []byte(prefixStr)
	for key, v := cursor.Seek(prefix); key != nil && bytes.HasPrefix(key, prefix); key, v = cursor.Next() {
		// we want to be able to grab buckets AND keys here
		kvs = append(kvs, &KeyValue{
			Key:   string(key),
			Value: append([]byte(nil), v...),
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
	tx, err := b.newTx(false)
	if err != nil {
		return nil, err
	}
	return &boltTXReadOnly{*tx}, nil
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

type boltTXReadOnly struct {
	boltTx
}

func (t *boltTXReadOnly) Bucket(name []byte) ReadOnlyTx {
	return &boltTXReadOnly{
		boltTx{
			b:  t.b.Bucket(name),
			tx: t.tx,
		}}
}

// BoltTx wraps an underlying bolt.Tx type to implement the Tx interface.
type boltTx struct {
	b  *Bolt
	tx *bolt.Tx
}

func (t *boltTx) Bucket(name []byte) Tx {
	return &boltTx{
		b:  t.b.Bucket(name),
		tx: t.tx,
	}
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
