package storage_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/influxdata/kapacitor/services/storage"
	"github.com/influxdata/kapacitor/services/storage/storagetest"
	"github.com/pkg/errors"
)

// Error used to specifically trigger a rollback for tests.
var rollbackErr = errors.New("rollback")

//type storeCloser interface {
//	Store(namespace string) storage.Interface
//	Close() error
//}

//type boltDB struct {
//	db  *bolt.DB
//	dir string
//}
//
//func (b boltDB) Close() error {
//	_ = b.db.Close() // we don't need to worry about this error
//	return os.RemoveAll(b.dir)
//}
//
//func newBolt() (storeCloser, error) {
//	tmpDir, err := os.MkdirTemp("", "storage-bolt")
//	if err != nil {
//		return nil, fmt.Errorf("failed to create temp directory: %v", err)
//	}
//	db, err := bolt.Open(filepath.Join(tmpDir, "bolt.db"), 0600, nil)
//	if err != nil {
//		return boltDB{}, err
//	}
//	return boltDB{
//		db:  db,
//		dir: tmpDir,
//	}, nil
//}
//
//func (b boltDB) Store(bucket string) storage.Interface {
//	return storage.NewBolt(b.db, []byte(bucket))
//}

func TestStorage_CRUD(t *testing.T) {
	t.Run("bolt", func(t *testing.T) {
		db, err := storagetest.NewBolt()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		s := db.Store("crud")
		s.Update(func(tx storage.Tx) error {
			key := "key0"
			value := []byte("test value")
			if exists, err := tx.Exists(key); err != nil {
				t.Fatal(err)
			} else if exists {
				t.Fatal("expected key to not exist")
			}

			if err := tx.Put(key, value); err != nil {
				t.Fatal(err)
			}
			if exists, err := tx.Exists(key); err != nil {
				t.Fatal(err)
			} else if !exists {
				t.Fatal("expected key to exist")
			}

			got, err := tx.Get(key)
			if err != nil {
				t.Fatal(err)
			}

			if !bytes.Equal(got.Value, value) {
				t.Fatalf("unexpected value got %q exp %q", string(got.Value), string(value))
			}

			if err := tx.Delete(key); err != nil {
				t.Fatal(err)
			}

			if exists, err := tx.Exists(key); err != nil {
				t.Fatal(err)
			} else if exists {
				t.Fatal("expected key to not exist after delete")
			}
			return nil
		})
	})
}

func TestStorage_Update(t *testing.T) {
	t.Run("bolt", func(t *testing.T) {
		db, err := storagetest.NewBolt()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		s := db.Store("commit")
		value := []byte("test value")
		err = s.Update(func(tx storage.Tx) error {
			return tx.Put("key0", value)
		})
		if err != nil {
			t.Fatal(err)
		}

		var got *storage.KeyValue
		err = s.View(func(tx storage.ReadOnlyTx) error {
			got, err = tx.Get("key0")
			return err
		})
		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(got.Value, value) {
			t.Errorf("unexpected value got %q exp %q", string(got.Value), string(value))
		}
	})
}

func TestStorage_Update_Rollback(t *testing.T) {
	t.Run("bolt", func(t *testing.T) {
		db, err := storagetest.NewBolt()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		s := db.Store("rollback")
		value := []byte("test value")

		// Put value
		err = s.Update(func(tx storage.Tx) error {
			return tx.Put("key0", value)
		})
		if err != nil {
			t.Fatal(err)
		}

		err = s.Update(func(tx storage.Tx) error {
			if err := tx.Put("key0", []byte("overridden value is rolledback")); err != nil {
				return err
			}
			return rollbackErr
		})

		if err == nil {
			t.Fatal("expected error")
		} else if err != rollbackErr {
			t.Fatalf("unexpected error: got %v exp %v", err, rollbackErr)
		}

		var got *storage.KeyValue
		s.View(func(tx storage.ReadOnlyTx) error {
			got, err = tx.Get("key0")
			return err
		})
		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(got.Value, value) {
			t.Errorf("unexpected value got %q exp %q", string(got.Value), string(value))
		}
	})
}

func TestStorage_Update_Concurrent(t *testing.T) {
	t.Run("bolt", func(t *testing.T) {
		db, err := storagetest.NewBolt()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		bucketFmt := func(w int) string {
			return fmt.Sprintf("bucket%d", w)
		}
		valueFmt := func(w, i, k int) []byte {
			return []byte(fmt.Sprintf("worker %d iteration %d key %d", w, i, k))
		}
		keyFmt := func(w, i, k int) string {
			return fmt.Sprintf("key%d", k)
		}

		putLoop := func(s storage.Interface, w, i, k int) error {
			// Begin new transaction
			err := s.Update(func(tx storage.Tx) error {
				// Put a set of values
				for x := 0; x < k; x++ {
					v := valueFmt(w, i, x)
					k := keyFmt(w, i, x)
					if err := tx.Put(k, v); err != nil {
						return err
					}
				}
				// Do not commit every third transaction
				if i%3 == 0 {
					return rollbackErr
				}
				return nil
			})
			// Mask explicit rollback errors
			if err == rollbackErr {
				err = nil
			}
			return err
		}

		testF := func(s storage.Interface, w, i, k int) error {
			for x := 0; x < i; x++ {
				if err := putLoop(s, w, x, k); err != nil {
					return errors.Wrapf(err, "worker %d", w)
				}
			}
			return nil
		}

		// Concurrency counts
		w := 10 // number of workers
		i := 10 // number of iterations
		k := 10 // number of keys to write

		errs := make(chan error, w)
		for x := 0; x < w; x++ {
			s := db.Store(bucketFmt(x))
			go func(s storage.Interface, w, i, k int) {
				errs <- testF(s, w, i, k)
			}(s, x, i, k)
		}
		for x := 0; x < w; x++ {
			err := <-errs
			if err != nil {
				t.Fatal(err)
			}
		}

		for x := 0; x < w; x++ {
			s := db.Store(bucketFmt(x))
			for z := 0; z < k; z++ {
				y := i - 1
				if y%3 == 0 {
					// The last iteration was not committed, expect the previous
					y--
				}
				key := keyFmt(x, y, z)
				value := valueFmt(x, y, z)
				var kv *storage.KeyValue
				err := s.View(func(tx storage.ReadOnlyTx) error {
					kv, err = tx.Get(key)
					return err
				})
				if err != nil {
					t.Fatalf("%s err:%v", key, err)
				}
				if !bytes.Equal(kv.Value, value) {
					t.Errorf("unexpected value for key %s: got %q exp %q", key, string(kv.Value), string(value))
				}
			}
		}
	})
}
