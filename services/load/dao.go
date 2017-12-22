package load

import (
	"encoding/json"
	"path"

	"github.com/influxdata/kapacitor/services/storage"
)

// version is the current version of the Item structure.
const version = 1

const (
	tasksStr     = "tasks"
	templatesStr = "templates"
	handlersStr  = "handlers"
)

// Data access object for resources loaded from
// a directory
type ItemsDAO interface {
	Set(i Item) error
	Delete(id string) error
	List(prefix string) ([]Item, error)

	Rebuild() error
}

type Item struct {
	ID string `json:"id"`
}

func newTaskItem(id string) Item {
	return Item{
		ID: path.Join(tasksStr, id),
	}
}

func newTemplateItem(id string) Item {
	return Item{
		ID: path.Join(templatesStr, id),
	}
}

func newTopicHandlerItem(topic, id string) Item {
	return Item{
		ID: path.Join(handlersStr, topic, id),
	}
}

func (i Item) MarshalBinary() ([]byte, error) {
	return storage.VersionJSONEncode(version, i)
}
func (i *Item) UnmarshalBinary(data []byte) error {
	return storage.VersionJSONDecode(data, func(version int, dec *json.Decoder) error {
		dec.UseNumber()
		return dec.Decode(i)
	})
}

func (i Item) ObjectID() string {
	return i.ID
}

// Key/Value store based implementation of ItemsDAO
type itemKV struct {
	store *storage.IndexedStore
}

func newItemKV(store storage.Interface) (*itemKV, error) {
	c := storage.DefaultIndexedStoreConfig("load_items", func() storage.BinaryObject {
		return new(Item)
	})

	istore, err := storage.NewIndexedStore(store, c)
	if err != nil {
		return nil, err
	}

	return &itemKV{
		store: istore,
	}, nil
}

func (kv *itemKV) Set(i Item) error {
	return kv.store.Put(&i)
}

func (kv *itemKV) Delete(id string) error {
	return kv.store.Delete(id)
}

func (kv *itemKV) List(prefix string) ([]Item, error) {
	objects, err := kv.store.List(path.Join(storage.DefaultIDIndex, prefix), "", 0, -1)
	if err != nil {
		return nil, err
	}
	items := make([]Item, len(objects))
	for i, object := range objects {
		it, ok := object.(*Item)
		if !ok {
			return nil, storage.ImpossibleTypeErr(i, object)
		}
		items[i] = *it
	}
	return items, nil
}

func (kv *itemKV) Rebuild() error {
	return kv.store.Rebuild()
}
