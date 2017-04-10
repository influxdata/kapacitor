package storage

import (
	"encoding"
	"fmt"
	"path"
	"strings"

	"github.com/pkg/errors"
)

const (
	defaultDataPrefix    = "data"
	defaultIndexesPrefix = "indexes"

	DefaultIDIndex = "id"
)

var (
	ErrObjectExists   = errors.New("object already exists")
	ErrNoObjectExists = errors.New("no object exists")
)

type BinaryObject interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
	ObjectID() string
}

type NewObjectF func() BinaryObject
type ValueFunc func(BinaryObject) (string, error)

type Index struct {
	Name      string
	ValueFunc ValueFunc
	Unique    bool
}

func (idx Index) ValueOf(o BinaryObject) (string, error) {
	value, err := idx.ValueFunc(o)
	if err != nil {
		return "", err
	}
	if !idx.Unique {
		value = value + "/" + o.ObjectID()
	}
	return value, nil
}

// Indexed provides basic CRUD operations and maintains indexes.
type IndexedStore struct {
	store Interface

	dataPrefix    string
	indexesPrefix string

	indexes []Index

	newObject NewObjectF
}

type IndexedStoreConfig struct {
	Prefix        string
	DataPrefix    string
	IndexesPrefix string
	NewObject     NewObjectF
	Indexes       []Index
}

func DefaultIndexedStoreConfig(prefix string, newObject NewObjectF) IndexedStoreConfig {
	return IndexedStoreConfig{
		Prefix:        prefix,
		DataPrefix:    defaultDataPrefix,
		IndexesPrefix: defaultIndexesPrefix,
		NewObject:     newObject,
		Indexes: []Index{{
			Name:   DefaultIDIndex,
			Unique: true,
			ValueFunc: func(o BinaryObject) (string, error) {
				return o.ObjectID(), nil
			},
		}},
	}
}

func validPath(p string) bool {
	return !strings.Contains(p, "/")
}

func (c IndexedStoreConfig) Validate() error {
	if c.Prefix == "" {
		return errors.New("must provide a prefix")
	}
	if !validPath(c.Prefix) {
		return fmt.Errorf("invalid prefix %q", c.Prefix)
	}
	if !validPath(c.DataPrefix) {
		return fmt.Errorf("invalid data prefix %q", c.DataPrefix)
	}
	if !validPath(c.IndexesPrefix) {
		return fmt.Errorf("invalid indexes prefix %q", c.IndexesPrefix)
	}
	if c.IndexesPrefix == c.DataPrefix {
		return fmt.Errorf("data prefix and indexes prefix must be different, both are %q", c.IndexesPrefix)
	}
	if c.NewObject == nil {
		return errors.New("must provide a NewObject function")
	}
	for _, idx := range c.Indexes {
		if !validPath(idx.Name) {
			return fmt.Errorf("invalid index name %q", idx.Name)
		}
		if idx.ValueFunc == nil {
			return fmt.Errorf("index %q does not have a ValueF function", idx.Name)
		}
	}
	return nil
}

func NewIndexedStore(store Interface, c IndexedStoreConfig) (*IndexedStore, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &IndexedStore{
		store:         store,
		dataPrefix:    path.Join("/", c.Prefix, c.DataPrefix) + "/",
		indexesPrefix: path.Join("/", c.Prefix, c.IndexesPrefix),
		indexes:       c.Indexes,
		newObject:     c.NewObject,
	}, nil
}

// Create a key for the object data
func (s *IndexedStore) dataKey(id string) string {
	return s.dataPrefix + id
}

// Create a key for a given index and value.
//
// Indexes are maintained via a 'directory' like system:
//
// /<dataPrefix>/<ID> -- contains encoded object data
// /<indexesPrefix>/id/<value> -- contains the object ID
//
// As such to list all handlers in ID sorted order use the /<indexesPrefix>/id/ directory.
func (s *IndexedStore) indexKey(index, value string) string {
	return path.Join(s.indexesPrefix, index, value)
}

func (s *IndexedStore) Get(id string) (o BinaryObject, err error) {
	err = s.store.View(func(tx ReadOnlyTx) error {
		o, err = s.GetTx(tx, id)
		return err
	})
	return
}

func (s *IndexedStore) GetTx(tx ReadOnlyTx, id string) (BinaryObject, error) {
	key := s.dataKey(id)
	if exists, err := tx.Exists(key); err != nil {
		return nil, err
	} else if !exists {
		return nil, ErrNoObjectExists
	}
	kv, err := tx.Get(key)
	if err != nil {
		return nil, err
	}
	o := s.newObject()
	err = o.UnmarshalBinary(kv.Value)
	return o, err
}

func (s *IndexedStore) Create(o BinaryObject) error {
	return s.put(o, false, false)
}
func (s *IndexedStore) CreateTx(tx Tx, o BinaryObject) error {
	return s.putTx(tx, o, false, false)
}

func (s *IndexedStore) Put(o BinaryObject) error {
	return s.put(o, true, false)
}
func (s *IndexedStore) PutTx(tx Tx, o BinaryObject) error {
	return s.putTx(tx, o, true, false)
}

func (s *IndexedStore) Replace(o BinaryObject) error {
	return s.put(o, true, true)
}
func (s *IndexedStore) ReplaceTx(tx Tx, o BinaryObject) error {
	return s.putTx(tx, o, true, true)
}

func (s *IndexedStore) put(o BinaryObject, allowReplace, requireReplace bool) error {
	return s.store.Update(func(tx Tx) error {
		return s.putTx(tx, o, allowReplace, requireReplace)
	})
}

func (s *IndexedStore) putTx(tx Tx, o BinaryObject, allowReplace, requireReplace bool) error {
	key := s.dataKey(o.ObjectID())

	replacing := false
	old, err := s.GetTx(tx, o.ObjectID())
	if err != nil {
		if err != ErrNoObjectExists || (requireReplace && err == ErrNoObjectExists) {
			return err
		}
	} else if !allowReplace {
		return ErrObjectExists
	} else {
		replacing = true
	}

	data, err := o.MarshalBinary()
	if err != nil {
		return err
	}

	// Put data
	err = tx.Put(key, data)
	if err != nil {
		return err
	}
	// Put all indexes
	for _, idx := range s.indexes {
		// Get new index key
		newValue, err := idx.ValueOf(o)
		if err != nil {
			return err
		}
		newIndexKey := s.indexKey(idx.Name, newValue)

		// Get old index key, if we are replacing
		var oldValue string
		if replacing {
			var err error
			oldValue, err = idx.ValueOf(old)
			if err != nil {
				return err
			}
		}
		oldIndexKey := s.indexKey(idx.Name, oldValue)

		if !replacing || (replacing && oldIndexKey != newIndexKey) {
			// Update new key
			err := tx.Put(newIndexKey, []byte(o.ObjectID()))
			if err != nil {
				return err
			}
			if replacing {
				// Remove old key
				err = tx.Delete(oldIndexKey)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (s *IndexedStore) Delete(id string) error {
	return s.store.Update(func(tx Tx) error {
		return s.DeleteTx(tx, id)
	})
}
func (s *IndexedStore) DeleteTx(tx Tx, id string) error {
	o, err := s.GetTx(tx, id)
	if err == ErrNoObjectExists {
		// Nothing to do
		return nil
	} else if err != nil {
		return err
	}

	// Delete object
	key := s.dataKey(id)
	err = tx.Delete(key)
	if err != nil {
		return err
	}

	// Delete all indexes
	for _, idx := range s.indexes {
		value, err := idx.ValueOf(o)
		if err != nil {
			return err
		}
		indexKey := s.indexKey(idx.Name, value)
		err = tx.Delete(indexKey)
		if err != nil {
			return err
		}
	}
	return nil
}

// List returns a list of objects that match a given pattern.
// If limit < 0, then no limit is enforced.
func (s *IndexedStore) List(index, pattern string, offset, limit int) (objects []BinaryObject, err error) {
	err = s.store.View(func(tx ReadOnlyTx) error {
		objects, err = s.list(tx, index, pattern, offset, limit, false)
		return err
	})
	return
}
func (s *IndexedStore) ListTx(tx ReadOnlyTx, index, pattern string, offset, limit int) ([]BinaryObject, error) {
	return s.list(tx, index, pattern, offset, limit, false)
}

// ReverseList returns a list of objects that match a given pattern, using reverse sort.
// If limit < 0, then no limit is enforced.
func (s *IndexedStore) ReverseList(index, pattern string, offset, limit int) (objects []BinaryObject, err error) {
	err = s.store.View(func(tx ReadOnlyTx) error {
		objects, err = s.list(tx, index, pattern, offset, limit, true)
		return err
	})
	return
}
func (s *IndexedStore) ReverseListTx(tx ReadOnlyTx, index, pattern string, offset, limit int) ([]BinaryObject, error) {
	return s.list(tx, index, pattern, offset, limit, true)
}

func (s *IndexedStore) list(tx ReadOnlyTx, index, pattern string, offset, limit int, reverse bool) ([]BinaryObject, error) {
	// List all object ids sorted by index
	ids, err := tx.List(s.indexKey(index, "") + "/")
	if err != nil {
		return nil, err
	}
	if reverse {
		// Reverse to sort
		for i, j := 0, len(ids)-1; i < j; i, j = i+1, j-1 {
			ids[i], ids[j] = ids[j], ids[i]
		}
	}

	var match func([]byte) bool
	if pattern != "" {
		match = func(value []byte) bool {
			id := string(value)
			matched, _ := path.Match(pattern, id)
			return matched
		}
	} else {
		match = func([]byte) bool { return true }
	}
	var matches []string
	if limit >= 0 {
		matches = DoListFunc(ids, match, offset, limit)
	} else {
		matches = make([]string, len(ids))
		for i := range ids {
			matches[i] = string(ids[i].Value)
		}
	}

	objects := make([]BinaryObject, len(matches))
	for i, id := range matches {
		data, err := tx.Get(s.dataKey(id))
		if err != nil {
			return nil, err
		}
		o := s.newObject()
		err = o.UnmarshalBinary(data.Value)
		if err != nil {
			return nil, err
		}
		objects[i] = o
	}
	return objects, nil
}

// Rebuild completely rebuilds all indexes for the store.
func (s *IndexedStore) Rebuild() error {
	return s.store.Update(func(tx Tx) error {
		return s.RebuildTx(tx)
	})
}

// Rebuild completely rebuilds all indexes for the store using the provided transaction.
func (s *IndexedStore) RebuildTx(tx Tx) error {
	// Delete all indexes
	for _, idx := range s.indexes {
		if err := s.deleteIndex(tx, idx.Name); err != nil {
			return errors.Wrapf(err, "failed to clean index %s", idx.Name)
		}
	}
	// Walk all data and update existing index entries
	data, err := tx.List(s.dataPrefix)
	if err != nil {
		return err
	}
	for _, kv := range data {
		o := s.newObject()
		err = o.UnmarshalBinary(kv.Value)
		if err != nil {
			return errors.Wrapf(err, "failed to unmarshal object with key: %q", kv.Key)
		}
		for _, idx := range s.indexes {
			v, err := idx.ValueOf(o)
			if err != nil {
				return errors.Wrapf(err, "failed to get index value for object with key: %q", kv.Key)
			}
			key := s.indexKey(idx.Name, v)
			err = tx.Put(key, []byte(o.ObjectID()))
			if err != nil {
				return errors.Wrapf(err, "failed to update index for object with key: %q", kv.Key)
			}
		}
	}
	return nil
}

// deleteIndex deletes all indexes entries.
func (s *IndexedStore) deleteIndex(tx Tx, index string) error {
	entries, err := tx.List(s.indexKey(index, "") + "/")
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := tx.Delete(entry.Key); err != nil {
			return err
		}
	}
	return nil
}

func ImpossibleTypeErr(exp interface{}, got interface{}) error {
	return fmt.Errorf("impossible error, object not of type %T, got %T", exp, got)
}
