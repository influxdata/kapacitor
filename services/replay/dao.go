package replay

import (
	"bytes"
	"encoding/gob"
	"errors"
	"time"

	"github.com/influxdata/kapacitor/services/storage"
)

var (
	ErrRecordingExists   = errors.New("recording already exists")
	ErrNoRecordingExists = errors.New("no recording exists")

	ErrReplayExists   = errors.New("replay already exists")
	ErrNoReplayExists = errors.New("no replay exists")
)

// Data access object for Recording data.
type RecordingDAO interface {
	// Retrieve a recording
	Get(id string) (Recording, error)

	// Create a recording.
	// ErrRecordingExists is returned if a recording already exists with the same ID.
	Create(recording Recording) error

	// Replace an existing recording.
	// ErrNoRecordingExists is returned if the recording does not exist.
	Replace(recording Recording) error

	// Delete a recording.
	// It is not an error to delete an non-existent recording.
	Delete(id string) error

	// List recordings matching a pattern.
	// The pattern is shell/glob matching see https://golang.org/pkg/path/#Match
	// Offset and limit are pagination bounds. Offset is inclusive starting at index 0.
	// More results may exist while the number of returned items is equal to limit.
	List(pattern string, offset, limit int) ([]Recording, error)

	// Rebuild fixes all indexes of the data.
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

type Status int

const (
	Failed Status = iota
	Running
	Finished
)

type RecordingType int

const (
	StreamRecording RecordingType = iota
	BatchRecording
)

type Recording struct {
	ID string
	// URL for stored Recording data. Currently only file:// is supported.
	DataURL  string
	Type     RecordingType
	Size     int64
	Date     time.Time
	Error    string
	Status   Status
	Progress float64
}

type rawRecording Recording

func (r Recording) ObjectID() string {
	return r.ID
}

func (r Recording) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode((rawRecording)(r))
	return buf.Bytes(), err
}

func (r *Recording) UnmarshalBinary(data []byte) error {
	return gob.NewDecoder(bytes.NewReader(data)).Decode((*rawRecording)(r))
}

// Name of Date index
const recordingDateIndex = "date"

// Key/Value based implementation of the RecordingDAO.
type recordingKV struct {
	store *storage.IndexedStore
}

func newRecordingKV(store storage.Interface) (*recordingKV, error) {
	c := storage.DefaultIndexedStoreConfig("recordings", func() storage.BinaryObject {
		return new(Recording)
	})
	c.Indexes = append(c.Indexes, storage.Index{
		Name: recordingDateIndex,
		ValueFunc: func(o storage.BinaryObject) (string, error) {
			r, ok := o.(*Recording)
			if !ok {
				return "", storage.ImpossibleTypeErr(r, o)
			}
			return r.Date.UTC().Format(time.RFC3339), nil
		},
	})
	istore, err := storage.NewIndexedStore(store, c)
	if err != nil {
		return nil, err
	}
	return &recordingKV{
		store: istore,
	}, nil
}

func (kv *recordingKV) Rebuild() error {
	return kv.store.Rebuild()
}

func (kv *recordingKV) error(err error) error {
	if err == storage.ErrNoObjectExists {
		return ErrNoRecordingExists
	} else if err == storage.ErrObjectExists {
		return ErrRecordingExists
	}
	return err
}

func (kv *recordingKV) Get(id string) (Recording, error) {
	o, err := kv.store.Get(id)
	if err != nil {
		return Recording{}, kv.error(err)
	}
	r, ok := o.(*Recording)
	if !ok {
		return Recording{}, storage.ImpossibleTypeErr(r, o)
	}
	return *r, nil
}

func (kv *recordingKV) Create(r Recording) error {
	return kv.error(kv.store.Create(&r))
}

func (kv *recordingKV) Replace(r Recording) error {
	return kv.error(kv.store.Replace(&r))
}

func (kv *recordingKV) Delete(id string) error {
	return kv.store.Delete(id)
}

func (kv *recordingKV) List(pattern string, offset, limit int) ([]Recording, error) {
	objects, err := kv.store.ReverseList(recordingDateIndex, pattern, offset, limit)
	if err != nil {
		return nil, err
	}
	recordings := make([]Recording, len(objects))
	for i, o := range objects {
		r, ok := o.(*Recording)
		if !ok {
			return nil, storage.ImpossibleTypeErr(r, o)
		}
		recordings[i] = *r
	}
	return recordings, nil
}

// Data access object for Recording data.
type ReplayDAO interface {
	// Retrieve a replay
	Get(id string) (Replay, error)

	// Create a replay.
	// ErrReplayExists is returned if a replay already exists with the same ID.
	Create(replay Replay) error

	// Replace an existing replay.
	// ErrNoReplayExists is returned if the replay does not exist.
	Replace(replay Replay) error

	// Delete a replay.
	// It is not an error to delete an non-existent replay.
	Delete(id string) error

	// List replays matching a pattern.
	// The pattern is shell/glob matching see https://golang.org/pkg/path/#Match
	// Offset and limit are pagination bounds. Offset is inclusive starting at index 0.
	// More results may exist while the number of returned items is equal to limit.
	List(pattern string, offset, limit int) ([]Replay, error)

	// Rebuild rebuilds all indexes for the storage
	Rebuild() error
}

type Clock int

const (
	Fast Clock = iota
	Real
)

type ExecutionStats struct {
	TaskStats map[string]interface{}
	NodeStats map[string]map[string]interface{}
}

type Replay struct {
	ID            string
	RecordingID   string
	TaskID        string
	RecordingTime bool
	Clock         Clock
	Date          time.Time
	Error         string
	Status        Status
	Progress      float64
	// Stores snapshot of finished replayed Task status
	ExecutionStats ExecutionStats
}

type rawReplay Replay

func (r Replay) ObjectID() string {
	return r.ID
}

func (r Replay) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(rawReplay(r))
	return buf.Bytes(), err
}

func (r *Replay) UnmarshalBinary(data []byte) error {
	return gob.NewDecoder(bytes.NewReader(data)).Decode((*rawReplay)(r))
}

// Name of the replay date index
const replayDateIndex = "date"

// Key/Value based implementation of the ReplayDAO.
type replayKV struct {
	store *storage.IndexedStore
}

func newReplayKV(store storage.Interface) (*replayKV, error) {
	c := storage.DefaultIndexedStoreConfig("replays", func() storage.BinaryObject {
		return new(Replay)
	})
	c.Indexes = append(c.Indexes, storage.Index{
		Name: replayDateIndex,
		ValueFunc: func(o storage.BinaryObject) (string, error) {
			r, ok := o.(*Replay)
			if !ok {
				return "", storage.ImpossibleTypeErr(r, o)
			}
			return r.Date.UTC().Format(time.RFC3339), nil
		},
	})
	istore, err := storage.NewIndexedStore(store, c)
	if err != nil {
		return nil, err
	}
	return &replayKV{
		store: istore,
	}, nil
}

func (kv *replayKV) error(err error) error {
	if err == storage.ErrNoObjectExists {
		return ErrNoReplayExists
	} else if err == storage.ErrObjectExists {
		return ErrReplayExists
	}
	return err
}

func (kv *replayKV) Get(id string) (Replay, error) {
	o, err := kv.store.Get(id)
	if err != nil {
		return Replay{}, kv.error(err)
	}
	r, ok := o.(*Replay)
	if !ok {
		return Replay{}, storage.ImpossibleTypeErr(r, o)
	}
	return *r, nil
}

func (kv *replayKV) Create(r Replay) error {
	return kv.error(kv.store.Create(&r))
}

func (kv *replayKV) Replace(r Replay) error {
	return kv.error(kv.store.Replace(&r))
}

func (kv *replayKV) Delete(id string) error {
	return kv.store.Delete(id)
}

func (kv *replayKV) List(pattern string, offset, limit int) ([]Replay, error) {
	objects, err := kv.store.ReverseList(replayDateIndex, pattern, offset, limit)
	if err != nil {
		return nil, err
	}
	replays := make([]Replay, len(objects))
	for i, o := range objects {
		r, ok := o.(*Replay)
		if !ok {
			return nil, storage.ImpossibleTypeErr(r, o)
		}
		replays[i] = *r
	}
	return replays, nil
}

func (kv *replayKV) Rebuild() error {
	return kv.store.Rebuild()
}
