package alert

//go:generate easyjson dao.go

import (
	"encoding/json"
	"fmt"
	"path"
	"regexp"
	"time"

	"github.com/mailru/easyjson/jlexer"

	"github.com/influxdata/kapacitor/alert"
	"github.com/influxdata/kapacitor/services/storage"
	"github.com/pkg/errors"
)

var (
	ErrHandlerSpecExists   = errors.New("handler spec already exists")
	ErrNoHandlerSpecExists = errors.New("no handler spec exists")
)

// Data access object for HandlerSpec data.
type HandlerSpecDAO interface {
	// Retrieve a handler
	Get(topic, id string) (HandlerSpec, error)
	GetTx(tx storage.ReadOperator, topic, id string) (HandlerSpec, error)

	// Create a handler.
	// ErrHandlerSpecExists is returned if a handler already exists with the same ID.
	Create(h HandlerSpec) error
	CreateTx(tx storage.Tx, h HandlerSpec) error

	// Replace an existing handler.
	// ErrNoHandlerSpecExists is returned if the handler does not exist.
	Replace(h HandlerSpec) error
	ReplaceTx(tx storage.Tx, h HandlerSpec) error

	// Delete a handler.
	// It is not an error to delete an non-existent handler.
	Delete(topic, id string) error
	DeleteTx(tx storage.Tx, topic, id string) error

	// List handlers matching a pattern.
	// The pattern is shell/glob matching see https://golang.org/pkg/path/#Match
	// Offset and limit are pagination bounds. Offset is inclusive starting at index 0.
	// More results may exist while the number of returned items is equal to limit.
	List(topic, pattern string, offset, limit int) ([]HandlerSpec, error)
	ListTx(tx storage.ReadOperator, topic, pattern string, offset, limit int) ([]HandlerSpec, error)

	Rebuild() error
}

//--------------------------------------------------------------------
// The following structures are stored in a database via gob encoding.
// Changes to the structures could break existing data.
//
// Many of these structures are exact copies of structures found elsewhere,
// this is intentional so that all structures stored in the database are
// defined here and nowhere else. So as to not accidentally change
// the gob serialization format in incompatible ways.

const (
	handlerSpecVersion1 = 1
	handlerSpecVersion2 = 2
)

// HandlerSpec provides all the necessary information to create a handler.
type HandlerSpec struct {
	ID      string                 `json:"id"`
	Topic   string                 `json:"topic"`
	Kind    string                 `json:"kind"`
	Options map[string]interface{} `json:"options"`
	Match   string                 `json:"match"`
}

var validHandlerID = regexp.MustCompile(`^[-\._\p{L}0-9]+$`)
var validTopicID = regexp.MustCompile(`^[-:\._\p{L}0-9]+$`)

func (h HandlerSpec) Validate() error {
	if !validTopicID.MatchString(h.Topic) {
		return fmt.Errorf("handler topic must contain only letters, numbers, '-', '.' and '_'. %q", h.ID)
	}
	if !validHandlerID.MatchString(h.ID) {
		return fmt.Errorf("handler ID must contain only letters, numbers, '-', '.' and '_'. %q", h.ID)
	}
	if h.Kind == "" {
		return errors.New("handler Kind must not be empty")
	}
	return nil
}

func fullID(topic, handler string) string {
	return path.Join(topic, handler)
}

func (h HandlerSpec) ObjectID() string {
	return fullID(h.Topic, h.ID)
}

func (h HandlerSpec) MarshalBinary() ([]byte, error) {
	if err := h.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid spec")
	}
	return storage.VersionJSONEncode(handlerSpecVersion2, h)
}

func (h *HandlerSpec) UnmarshalBinary(data []byte) error {
	return storage.VersionJSONDecode(data, func(version int, dec *json.Decoder) error {
		switch version {
		case handlerSpecVersion1:
			return errors.New("version 1 is invalid cannot decode")
		case handlerSpecVersion2:
			return dec.Decode(h)
		default:
			return fmt.Errorf("unknown spec version %d: cannot decode", version)
		}
	})
}

// Key/Value store based implementation of the HandlerSpecDAO
type handlerSpecKV struct {
	store *storage.IndexedStore
}

const (
	handlerPrefix = "handlers"
)

func newHandlerSpecKV(store storage.Interface) (*handlerSpecKV, error) {
	c := storage.DefaultIndexedStoreConfig(handlerPrefix, func() storage.BinaryObject {
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

func (kv *handlerSpecKV) Get(topic, id string) (HandlerSpec, error) {
	return kv.getHelper(kv.store.Get(fullID(topic, id)))
}

func (kv *handlerSpecKV) GetTx(tx storage.ReadOperator, topic, id string) (HandlerSpec, error) {
	return kv.getHelper(kv.store.GetTx(tx, fullID(topic, id)))
}

func (kv *handlerSpecKV) getHelper(o storage.BinaryObject, err error) (HandlerSpec, error) {
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
	return kv.store.Put(&h)
}
func (kv *handlerSpecKV) CreateTx(tx storage.Tx, h HandlerSpec) error {
	return kv.store.CreateTx(tx, &h)
}

func (kv *handlerSpecKV) Replace(h HandlerSpec) error {
	return kv.store.Replace(&h)
}
func (kv *handlerSpecKV) ReplaceTx(tx storage.Tx, h HandlerSpec) error {
	return kv.store.ReplaceTx(tx, &h)
}

func (kv *handlerSpecKV) Delete(topic, id string) error {
	return kv.store.Delete(fullID(topic, id))
}
func (kv *handlerSpecKV) DeleteTx(tx storage.Tx, topic, id string) error {
	return kv.store.DeleteTx(tx, fullID(topic, id))
}

func (kv *handlerSpecKV) List(topic, pattern string, offset, limit int) ([]HandlerSpec, error) {
	if pattern == "" {
		pattern = "*"
	}
	return kv.listHelper(kv.store.List(storage.DefaultIDIndex, fullID(topic, pattern), offset, limit))
}
func (kv *handlerSpecKV) ListTx(tx storage.ReadOperator, topic, pattern string, offset, limit int) ([]HandlerSpec, error) {
	if pattern == "" {
		pattern = "*"
	}
	return kv.listHelper(kv.store.ListTx(tx, storage.DefaultIDIndex, fullID(topic, pattern), offset, limit))
}
func (kv *handlerSpecKV) listHelper(objects []storage.BinaryObject, err error) ([]HandlerSpec, error) {
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

func (kv *handlerSpecKV) Rebuild() error {
	return kv.store.Rebuild()
}

var (
	ErrNoTopicStateExists = errors.New("no topic state exists")
)

//// Data access object for TopicState data.
//type TopicStateDAO interface {
//	// Retrieve a handler
//	Get(id string) (TopicState, error)
//
//	// Put an alert.Event, replaces any existing alert.Event.
//	Put(h alert.Event) error
//
//	// Delete a handler.
//	// It is not an error to delete an non-existent handler.
//	Delete(id string) error
//
//	// List handlers matching a pattern.
//	// The pattern is shell/glob matching see https://golang.org/pkg/path/#Match
//	// Offset and limit are pagination bounds. Offset is inclusive starting at index 0.
//	// More results may exist while the number of returned items is equal to limit.
//	List(pattern string, offset, limit int) ([]TopicState, error)
//
//	Rebuild() error
//}

const topicStateVersion = 1

//easyjson:json
type TopicState struct {
	Topic       string                `json:"topic"`
	EventStates map[string]EventState `json:"event-states"`
}

//easyjson:json
type EventState struct {
	Message  string        `json:"message,omitempty"`
	Details  string        `json:"details,omitempty"`
	Time     time.Time     `json:"time,omitempty"`
	Duration time.Duration `json:"duration,omitempty"`
	Level    alert.Level   `json:"level"`
}

func (e *EventState) Reset() {
	e.Message = ""
	e.Details = ""
	e.Time = time.Time{}
	e.Duration = 0
	e.Level = 0
}

func (e *EventState) AlertEventState(id string) *alert.EventState {
	return &alert.EventState{
		ID:       id,
		Message:  e.Message,
		Details:  e.Details,
		Time:     e.Time,
		Duration: e.Duration,
		Level:    e.Level,
	}
}

//type DurableEventState struct {
//	*EventState
//	Topic string
//	ID    string
//}
//
//func (t DurableEventState) ObjectID() string {
//	return t.Topic
//}

func (t TopicState) ObjectID() string {
	return t.Topic
}

func (t TopicState) MarshalBinary() ([]byte, error) {
	return storage.VersionJSONEncode(topicStateVersion, t)
}

func (t *TopicState) UnmarshalBinary(data []byte) error {
	return storage.VersionEasyJSONDecode(data, func(version int, dec *jlexer.Lexer) error {
		t.UnmarshalEasyJSON(dec)
		return dec.Error()
	})
}
