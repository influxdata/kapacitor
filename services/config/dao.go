package config

import (
	"encoding/json"
	"errors"

	"github.com/influxdata/kapacitor/services/storage"
)

var (
	ErrNoOverrideExists = errors.New("no override exists")
)

// Data access object for Override data.
type OverrideDAO interface {
	// Retrieve a override
	Get(id string) (Override, error)

	// Set an override.
	// If it does not already exist it will be created,
	// otherwise it will be replaced.
	Set(o Override) error

	// Delete a override.
	// It is not an error to delete an non-existent override.
	Delete(id string) error

	// List all overrides whose ID starts with the given prefix
	List(prefix string) ([]Override, error)

	Rebuild() error
}

//--------------------------------------------------------------------
// The following structures are stored in a database via JSON encoding.
// Changes to the structures could break existing data.
//
// Many of these structures are exact copies of structures found elsewhere,
// this is intentional so that all structures stored in the database are
// defined here and nowhere else. So as to not accidentally change
// the JSON serialization format in incompatible ways.

// version is the current version of the Override structure.
const version = 1

type Override struct {
	// Unique identifier for the override
	ID string `json:"id"`

	// Map of key value pairs of option overrides.
	Options map[string]interface{} `json:"options"`

	Create bool `json:"create"`
}

func (o Override) ObjectID() string {
	return o.ID
}

func (o Override) MarshalBinary() ([]byte, error) {
	return storage.VersionJSONEncode(version, o)
}

func (o *Override) UnmarshalBinary(data []byte) error {
	return storage.VersionJSONDecode(data, func(version int, dec *json.Decoder) error {
		dec.UseNumber()
		return dec.Decode(o)
	})
}

// Key/Value store based implementation of the OverrideDAO
type overrideKV struct {
	store *storage.IndexedStore
}

func newOverrideKV(store storage.Interface) (*overrideKV, error) {
	c := storage.DefaultIndexedStoreConfig("overrides", func() storage.BinaryObject {
		return new(Override)
	})
	istore, err := storage.NewIndexedStore(store, c)
	if err != nil {
		return nil, err
	}
	return &overrideKV{
		store: istore,
	}, nil
}

func (kv *overrideKV) error(err error) error {
	if err == storage.ErrNoObjectExists {
		return ErrNoOverrideExists
	}
	return err
}

func (kv *overrideKV) Get(id string) (Override, error) {
	obj, err := kv.store.Get(id)
	if err != nil {
		return Override{}, kv.error(err)
	}
	o, ok := obj.(*Override)
	if !ok {
		return Override{}, storage.ImpossibleTypeErr(o, obj)
	}
	return *o, nil
}

func (kv *overrideKV) Set(o Override) error {
	return kv.store.Put(&o)
}

func (kv *overrideKV) Delete(id string) error {
	return kv.store.Delete(id)
}

func (kv *overrideKV) List(prefix string) ([]Override, error) {
	objects, err := kv.store.List(storage.DefaultIDIndex, "", 0, -1)
	if err != nil {
		return nil, err
	}
	overrides := make([]Override, len(objects))
	for i, object := range objects {
		o, ok := object.(*Override)
		if !ok {
			return nil, storage.ImpossibleTypeErr(o, object)
		}
		overrides[i] = *o
	}
	return overrides, nil
}

func (kv *overrideKV) Rebuild() error {
	return kv.store.Rebuild()
}
