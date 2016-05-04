package replay

import (
	"bytes"
	"encoding/gob"
	"errors"
	"path"
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

const (
	recordingDataPrefix    = "/recordings/data/"
	recordingIndexesPrefix = "/recordings/indexes/"

	// Name of ID index
	recordingIdIndex = "id/"
	// Name of Date index
	recordingDateIndex = "date/"
)

// Key/Value based implementation of the RecordingDAO.
type recordingKV struct {
	store storage.Interface
}

func newRecordingKV(store storage.Interface) *recordingKV {
	return &recordingKV{
		store: store,
	}
}

func (d *recordingKV) encodeRecording(r Recording) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(r)
	return buf.Bytes(), err
}

func (d *recordingKV) decodeRecording(data []byte) (Recording, error) {
	var recording Recording
	dec := gob.NewDecoder(bytes.NewReader(data))
	err := dec.Decode(&recording)
	return recording, err
}

// Create a key for the recording data
func (d *recordingKV) recordingDataKey(id string) string {
	return recordingDataPrefix + id
}

// Create a key for a given index and value.
//
// Indexes are maintained via a 'directory' like system:
//
// /recordings/data/ID -- contains encoded recording data
// /recordings/index/id/ID -- contains the recording ID
// /recordings/index/date/DATE/ID -- contains the recording ID
//
// As such to list all recordings in Date sorted order use the /recordings/index/date/ directory.
func (d *recordingKV) recordingIndexKey(index, value string) string {
	return recordingIndexesPrefix + index + value
}

func (d *recordingKV) recordingIDIndexKey(r Recording) string {
	return d.recordingIndexKey(replayIdIndex, r.ID)
}
func (d *recordingKV) recordingDateIndexKey(r Recording) string {
	return d.recordingIndexKey(recordingDateIndex, r.Date.Format(time.RFC3339)+"/"+r.ID)
}

func (d *recordingKV) Get(id string) (Recording, error) {
	key := d.recordingDataKey(id)
	if exists, err := d.store.Exists(key); err != nil {
		return Recording{}, err
	} else if !exists {
		return Recording{}, ErrNoRecordingExists
	}
	kv, err := d.store.Get(key)
	if err != nil {
		return Recording{}, err
	}
	return d.decodeRecording(kv.Value)
}

func (d *recordingKV) Create(r Recording) error {
	key := d.recordingDataKey(r.ID)

	exists, err := d.store.Exists(key)
	if err != nil {
		return err
	}
	if exists {
		return ErrRecordingExists
	}

	data, err := d.encodeRecording(r)
	if err != nil {
		return err
	}
	// Put data
	err = d.store.Put(key, data)
	if err != nil {
		return err
	}
	// Put ID index
	indexKey := d.recordingIDIndexKey(r)
	err = d.store.Put(indexKey, []byte(r.ID))
	if err != nil {
		return err
	}
	// Put Date index
	indexKey = d.recordingDateIndexKey(r)
	return d.store.Put(indexKey, []byte(r.ID))
}

func (d *recordingKV) Replace(r Recording) error {
	key := d.recordingDataKey(r.ID)

	exists, err := d.store.Exists(key)
	if err != nil {
		return err
	}
	if !exists {
		return ErrNoRecordingExists
	}

	prev, err := d.Get(r.ID)
	if err != nil {
		return err
	}

	data, err := d.encodeRecording(r)
	if err != nil {
		return err
	}
	// Put data
	err = d.store.Put(key, data)
	if err != nil {
		return err
	}
	// Update Date index
	prevIndexKey := d.recordingDateIndexKey(prev)
	err = d.store.Delete(prevIndexKey)
	if err != nil {
		return err
	}
	currIndexKey := d.recordingDateIndexKey(r)
	err = d.store.Put(currIndexKey, []byte(r.ID))
	if err != nil {
		return err
	}
	return nil
}

func (d *recordingKV) Delete(id string) error {
	key := d.recordingDataKey(id)
	r, err := d.Get(id)
	if err != nil {
		if err == ErrNoRecordingExists {
			return nil
		}
		return err
	}

	idIndexKey := d.recordingIDIndexKey(r)
	dateIndexKey := d.recordingDateIndexKey(r)

	dataErr := d.store.Delete(key)
	idIndexErr := d.store.Delete(idIndexKey)
	dateIndexErr := d.store.Delete(dateIndexKey)
	if dataErr != nil {
		return dataErr
	}
	if idIndexErr != nil {
		return dataErr
	}
	return dateIndexErr
}

func (d *recordingKV) List(pattern string, offset, limit int) ([]Recording, error) {
	// Recordings are indexed by their Date.
	// This allows us to do offset/limits and filtering without having to read in all recording data.

	// List all recording ids sorted by Date
	ids, err := d.store.List(recordingIndexesPrefix + recordingDateIndex)
	if err != nil {
		return nil, err
	}
	// Reverse to sort by newest first
	for i, j := 0, len(ids)-1; i < j; i, j = i+1, j-1 {
		ids[i], ids[j] = ids[j], ids[i]
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
	matches := storage.DoListFunc(ids, match, offset, limit)

	recordings := make([]Recording, len(matches))
	for i, id := range matches {
		data, err := d.store.Get(d.recordingDataKey(string(id)))
		if err != nil {
			return nil, err
		}
		t, err := d.decodeRecording(data.Value)
		recordings[i] = t
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
}

type Clock int

const (
	Fast Clock = iota
	Real
)

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
}

const (
	replayDataPrefix    = "/replays/data/"
	replayIndexesPrefix = "/replays/indexes/"

	replayIdIndex   = "id/"
	replayDateIndex = "date/"
)

// Key/Value based implementation of the ReplayDAO.
type replayKV struct {
	store storage.Interface
}

func newReplayKV(store storage.Interface) *replayKV {
	return &replayKV{
		store: store,
	}
}

func (d *replayKV) encodeReplay(r Replay) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(r)
	return buf.Bytes(), err
}

func (d *replayKV) decodeReplay(data []byte) (Replay, error) {
	var replay Replay
	dec := gob.NewDecoder(bytes.NewReader(data))
	err := dec.Decode(&replay)
	return replay, err
}

// Create a key for the replay data
func (d *replayKV) replayDataKey(id string) string {
	return replayDataPrefix + id
}

// Create a key for a given index and value.
//
// Indexes are maintained via a 'directory' like system:
//
// /replays/data/ID -- contains encoded replay data
// /replays/index/id/ID -- contains the replay ID
// /replays/index/date/DATE/ID -- contains the replay ID
//
// As such to list all replays in Date sorted order use the /replays/index/date/ directory.
func (d *replayKV) replayIndexKey(index, value string) string {
	return replayIndexesPrefix + index + value
}

func (d *replayKV) replayIDIndexKey(r Replay) string {
	return d.replayIndexKey(replayIdIndex, r.ID)
}
func (d *replayKV) replayDateIndexKey(r Replay) string {
	return d.replayIndexKey(replayDateIndex, r.Date.Format(time.RFC3339)+"/"+r.ID)
}

func (d *replayKV) Get(id string) (Replay, error) {
	key := d.replayDataKey(id)
	if exists, err := d.store.Exists(key); err != nil {
		return Replay{}, err
	} else if !exists {
		return Replay{}, ErrNoReplayExists
	}
	kv, err := d.store.Get(key)
	if err != nil {
		return Replay{}, err
	}
	return d.decodeReplay(kv.Value)
}

func (d *replayKV) Create(r Replay) error {
	key := d.replayDataKey(r.ID)

	exists, err := d.store.Exists(key)
	if err != nil {
		return err
	}
	if exists {
		return ErrReplayExists
	}

	data, err := d.encodeReplay(r)
	if err != nil {
		return err
	}
	// Put data
	err = d.store.Put(key, data)
	if err != nil {
		return err
	}
	// Put ID index
	indexKey := d.replayIDIndexKey(r)
	err = d.store.Put(indexKey, []byte(r.ID))
	if err != nil {
		return err
	}
	// Put Date index
	indexKey = d.replayDateIndexKey(r)
	return d.store.Put(indexKey, []byte(r.ID))
}

func (d *replayKV) Replace(r Replay) error {
	key := d.replayDataKey(r.ID)

	exists, err := d.store.Exists(key)
	if err != nil {
		return err
	}
	if !exists {
		return ErrNoReplayExists
	}

	prev, err := d.Get(r.ID)
	if err != nil {
		return err
	}

	data, err := d.encodeReplay(r)
	if err != nil {
		return err
	}
	// Put data
	err = d.store.Put(key, data)
	if err != nil {
		return err
	}
	// Update Date index
	prevIndexKey := d.replayDateIndexKey(prev)
	err = d.store.Delete(prevIndexKey)
	if err != nil {
		return err
	}
	currIndexKey := d.replayDateIndexKey(r)
	err = d.store.Put(currIndexKey, []byte(r.ID))
	if err != nil {
		return err
	}
	return nil
}

func (d *replayKV) Delete(id string) error {
	key := d.replayDataKey(id)
	r, err := d.Get(id)
	if err != nil {
		if err == ErrNoReplayExists {
			return nil
		}
		return err
	}

	idIndexKey := d.replayIDIndexKey(r)
	dateIndexKey := d.replayDateIndexKey(r)

	dataErr := d.store.Delete(key)
	idIndexErr := d.store.Delete(idIndexKey)
	dateIndexErr := d.store.Delete(dateIndexKey)
	if dataErr != nil {
		return dataErr
	}
	if idIndexErr != nil {
		return dataErr
	}
	return dateIndexErr
}

func (d *replayKV) List(pattern string, offset, limit int) ([]Replay, error) {
	// Replays are indexed by their Date.
	// This allows us to do offset/limits and filtering without having to read in all replay data.

	// List all replay ids sorted by Date
	ids, err := d.store.List(replayIndexesPrefix + replayDateIndex)
	if err != nil {
		return nil, err
	}
	// Reverse to sort by newest first
	for i, j := 0, len(ids)-1; i < j; i, j = i+1, j-1 {
		ids[i], ids[j] = ids[j], ids[i]
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
	matches := storage.DoListFunc(ids, match, offset, limit)

	replays := make([]Replay, len(matches))
	for i, id := range matches {
		data, err := d.store.Get(d.replayDataKey(string(id)))
		if err != nil {
			return nil, err
		}
		t, err := d.decodeReplay(data.Value)
		replays[i] = t
	}
	return replays, nil
}
