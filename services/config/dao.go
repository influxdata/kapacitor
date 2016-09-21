package config

import (
	"bytes"
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
}

//--------------------------------------------------------------------
// The following structures are stored in a database via gob encoding.
// Changes to the structures could break existing data.
//
// Many of these structures are exact copies of structures found elsewhere,
// this is intentional so that all structures stored in the database are
// defined here and nowhere else. So as to not accidentally change
// the gob serialization format in incompatible ways.

type Override struct {
	// Unique identifier for the override
	ID string

	// Map of key value pairs of option overrides.
	Options map[string]interface{}

	Create bool
}

const (
	overrideDataPrefix    = "/overrides/data/"
	overrideIndexesPrefix = "/overrides/indexes/"

	// Name of ID index
	idIndex = "id/"
)

// Key/Value store based implementation of the OverrideDAO
type overrideKV struct {
	store storage.Interface
}

func newOverrideKV(store storage.Interface) *overrideKV {
	return &overrideKV{
		store: store,
	}
}

func (d *overrideKV) encodeOverride(o Override) ([]byte, error) {
	var buf bytes.Buffer
	// Using JSON encoding since gob doesn't handle arbitrary interfaces well,
	// and this data is shipped over the wire via JSON so we know it will work.
	enc := json.NewEncoder(&buf)
	err := enc.Encode(o)
	return buf.Bytes(), err
}

func (d *overrideKV) decodeOverride(data []byte) (Override, error) {
	var override Override
	dec := json.NewDecoder(bytes.NewReader(data))
	// Do not convert all nums to float64, rather use json.Number which is a Stringer
	dec.UseNumber()
	err := dec.Decode(&override)
	return override, err
}

// Create a key for the override data
func (d *overrideKV) overrideDataKey(id string) string {
	return overrideDataPrefix + id
}

// Create a key for a given index and value.
//
// Indexes are maintained via a 'directory' like system:
//
// /overrides/data/ID -- contains encoded override data
// /overrides/index/id/ID -- contains the override ID
//
// As such to list all overrides in ID sorted order use the /overrides/index/id/ directory.
func (d *overrideKV) overrideIndexKey(index, value string) string {
	return overrideIndexesPrefix + index + value
}

func (d *overrideKV) Get(id string) (Override, error) {
	key := d.overrideDataKey(id)
	if exists, err := d.store.Exists(key); err != nil {
		return Override{}, err
	} else if !exists {
		return Override{}, ErrNoOverrideExists
	}
	kv, err := d.store.Get(key)
	if err != nil {
		return Override{}, err
	}
	return d.decodeOverride(kv.Value)
}

func (d *overrideKV) Set(o Override) error {
	key := d.overrideDataKey(o.ID)

	data, err := d.encodeOverride(o)
	if err != nil {
		return err
	}
	// Put data
	err = d.store.Put(key, data)
	if err != nil {
		return err
	}
	// Put ID index
	indexKey := d.overrideIndexKey(idIndex, o.ID)
	return d.store.Put(indexKey, []byte(o.ID))
}

func (d *overrideKV) Delete(id string) error {
	key := d.overrideDataKey(id)
	indexKey := d.overrideIndexKey(idIndex, id)

	dataErr := d.store.Delete(key)
	indexErr := d.store.Delete(indexKey)
	if dataErr != nil {
		return dataErr
	}
	return indexErr
}

func (d *overrideKV) List(prefix string) ([]Override, error) {
	// List all override ids sorted by ID
	ids, err := d.store.List(overrideIndexesPrefix + idIndex + prefix)
	if err != nil {
		return nil, err
	}
	overrides := make([]Override, 0, len(ids))
	for _, kv := range ids {
		id := string(kv.Value)
		o, err := d.Get(id)
		if err != nil {
			return nil, err
		}
		overrides = append(overrides, o)
	}

	return overrides, nil
}
