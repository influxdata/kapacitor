package alert

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/influxdata/kapacitor/alert"
	"github.com/influxdata/kapacitor/services/storage"
)

var (
	ErrHandlerSpecExists   = errors.New("handler spec already exists")
	ErrNoHandlerSpecExists = errors.New("no handler spec exists")
)

// Data access object for HandlerSpec data.
type HandlerSpecDAO interface {
	// Retrieve a handler
	Get(id string) (HandlerSpec, error)

	// Create a handler.
	// ErrHandlerSpecExists is returned if a handler already exists with the same ID.
	Create(h HandlerSpec) error

	// Replace an existing handler.
	// ErrNoHandlerSpecExists is returned if the handler does not exist.
	Replace(h HandlerSpec) error

	// Delete a handler.
	// It is not an error to delete an non-existent handler.
	Delete(id string) error

	// List handlers matching a pattern.
	// The pattern is shell/glob matching see https://golang.org/pkg/path/#Match
	// Offset and limit are pagination bounds. Offset is inclusive starting at index 0.
	// More results may exist while the number of returned items is equal to limit.
	List(pattern string, offset, limit int) ([]HandlerSpec, error)
}

//--------------------------------------------------------------------
// The following structures are stored in a database via gob encoding.
// Changes to the structures could break existing data.
//
// Many of these structures are exact copies of structures found elsewhere,
// this is intentional so that all structures stored in the database are
// defined here and nowhere else. So as to not accidentally change
// the gob serialization format in incompatible ways.

// version is the current version of the HandlerSpec structure.
const handlerSpecVersion = 1

// HandlerSpec provides all the necessary information to create a handler.
type HandlerSpec struct {
	ID      string              `json:"id"`
	Topics  []string            `json:"topics"`
	Actions []HandlerActionSpec `json:"actions"`
}

var validHandlerID = regexp.MustCompile(`^[-\._\p{L}0-9]+$`)
var validTopicID = regexp.MustCompile(`^[-:\._\p{L}0-9]+$`)

func (h HandlerSpec) Validate() error {
	if !validHandlerID.MatchString(h.ID) {
		return fmt.Errorf("handler ID must contain only letters, numbers, '-', '.' and '_'. %q", h.ID)
	}
	for _, t := range h.Topics {
		if !validTopicID.MatchString(t) {
			return fmt.Errorf("topic must contain only letters, numbers, '-', '.', ':' and '_'. %q", t)
		}
	}
	if len(h.Actions) == 0 {
		return errors.New("must provide at least one action")
	}
	return nil
}

// HandlerActionSpec defines an action an handler can take.
type HandlerActionSpec struct {
	Kind    string                 `json:"kind"`
	Options map[string]interface{} `json:"options"`
}

func (h HandlerSpec) ObjectID() string {
	return h.ID
}

func (h HandlerSpec) MarshalBinary() ([]byte, error) {
	return storage.VersionJSONEncode(handlerSpecVersion, h)
}

func (h *HandlerSpec) UnmarshalBinary(data []byte) error {
	return storage.VersionJSONDecode(data, func(version int, dec *json.Decoder) error {
		return dec.Decode(h)
	})
}

// Key/Value store based implementation of the HandlerSpecDAO
type handlerSpecKV struct {
	store *storage.IndexedStore
}

func newHandlerSpecKV(store storage.Interface) (*handlerSpecKV, error) {
	c := storage.DefaultIndexedStoreConfig("handlers", func() storage.BinaryObject {
		return new(HandlerSpec)
	})
	istore, err := storage.NewIndexedStore(store, c)
	if err != nil {
		return nil, err
	}
	return &handlerSpecKV{
		store: istore,
	}, nil
}

func (kv *handlerSpecKV) error(err error) error {
	if err == storage.ErrObjectExists {
		return ErrHandlerSpecExists
	} else if err == storage.ErrNoObjectExists {
		return ErrNoHandlerSpecExists
	}
	return err
}

func (kv *handlerSpecKV) Get(id string) (HandlerSpec, error) {
	o, err := kv.store.Get(id)
	if err != nil {
		return HandlerSpec{}, kv.error(err)
	}
	h, ok := o.(*HandlerSpec)
	if !ok {
		return HandlerSpec{}, storage.ImpossibleTypeErr(h, o)
	}
	return *h, nil
}

func (kv *handlerSpecKV) Create(h HandlerSpec) error {
	return kv.store.Create(&h)
}

func (kv *handlerSpecKV) Replace(h HandlerSpec) error {
	return kv.store.Replace(&h)
}

func (kv *handlerSpecKV) Delete(id string) error {
	return kv.store.Delete(id)
}

func (kv *handlerSpecKV) List(pattern string, offset, limit int) ([]HandlerSpec, error) {
	objects, err := kv.store.List(storage.DefaultIDIndex, pattern, offset, limit)
	if err != nil {
		return nil, err
	}
	specs := make([]HandlerSpec, len(objects))
	for i, o := range objects {
		h, ok := o.(*HandlerSpec)
		if !ok {
			return nil, storage.ImpossibleTypeErr(h, o)
		}
		specs[i] = *h
	}
	return specs, nil
}

var (
	ErrNoTopicStateExists = errors.New("no topic state exists")
)

// Data access object for TopicState data.
type TopicStateDAO interface {
	// Retrieve a handler
	Get(id string) (TopicState, error)

	// Put a topic state, replaces any existing state.
	Put(h TopicState) error

	// Delete a handler.
	// It is not an error to delete an non-existent handler.
	Delete(id string) error

	// List handlers matching a pattern.
	// The pattern is shell/glob matching see https://golang.org/pkg/path/#Match
	// Offset and limit are pagination bounds. Offset is inclusive starting at index 0.
	// More results may exist while the number of returned items is equal to limit.
	List(pattern string, offset, limit int) ([]TopicState, error)
}

const topicStateVersion = 1

type TopicState struct {
	Topic       string                `json:"topic"`
	EventStates map[string]EventState `json:"event-states"`
}

type EventState struct {
	Message  string        `json:"message"`
	Details  string        `json:"details"`
	Time     time.Time     `json:"time"`
	Duration time.Duration `json:"duration"`
	Level    alert.Level   `json:"level"`
}

func (t TopicState) ObjectID() string {
	return t.Topic
}

func (t TopicState) MarshalBinary() ([]byte, error) {
	return storage.VersionJSONEncode(topicStateVersion, t)
}

func (t *TopicState) UnmarshalBinary(data []byte) error {
	return storage.VersionJSONDecode(data, func(version int, dec *json.Decoder) error {
		return dec.Decode(&t)
	})
}

// Key/Value store based implementation of the TopicStateDAO
type topicStateKV struct {
	store *storage.IndexedStore
}

func newTopicStateKV(store storage.Interface) (*topicStateKV, error) {
	c := storage.DefaultIndexedStoreConfig("topics", func() storage.BinaryObject {
		return new(TopicState)
	})
	istore, err := storage.NewIndexedStore(store, c)
	if err != nil {
		return nil, err
	}
	return &topicStateKV{
		store: istore,
	}, nil
}

func (kv *topicStateKV) error(err error) error {
	if err == storage.ErrNoObjectExists {
		return ErrNoTopicStateExists
	}
	return err
}

func (kv *topicStateKV) Get(id string) (TopicState, error) {
	o, err := kv.store.Get(id)
	if err != nil {
		return TopicState{}, kv.error(err)
	}
	t, ok := o.(*TopicState)
	if !ok {
		return TopicState{}, storage.ImpossibleTypeErr(t, o)
	}
	return *t, nil
}

func (kv *topicStateKV) Put(t TopicState) error {
	return kv.store.Put(&t)
}

func (kv *topicStateKV) Replace(t TopicState) error {
	return kv.store.Replace(&t)
}

func (kv *topicStateKV) Delete(id string) error {
	return kv.store.Delete(id)
}

func (kv *topicStateKV) List(pattern string, offset, limit int) ([]TopicState, error) {
	objects, err := kv.store.List(storage.DefaultIDIndex, pattern, offset, limit)
	if err != nil {
		return nil, err
	}
	specs := make([]TopicState, len(objects))
	for i, o := range objects {
		t, ok := o.(*TopicState)
		if !ok {
			return nil, storage.ImpossibleTypeErr(t, o)
		}
		specs[i] = *t
	}
	return specs, nil
}
