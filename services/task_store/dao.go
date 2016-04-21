package task_store

import (
	"bytes"
	"encoding/gob"
	"errors"
	"path"

	"github.com/influxdata/kapacitor/services/storage"
)

var (
	ErrTaskExists       = errors.New("task already exists")
	ErrNoTaskExists     = errors.New("no task exists")
	ErrNoSnapshotExists = errors.New("no snapshot exists")
)

// Data access object for Task Snapshot data.
type TaskDAO interface {
	// Retrieve a task
	Get(id string) (Task, error)

	// Create a task.
	// ErrTaskExists is returned if a task already exists with the same ID.
	Create(t Task) error

	// Replace an existing task.
	// ErrNoTaskExists is returned if the task does not exist.
	Replace(t Task) error

	// Delete a task.
	// It is not an error to delete an non-existent task.
	Delete(id string) error

	// List tasks matching a pattern.
	// The pattern is shell/glob matching see https://golang.org/pkg/path/#Match
	// Offset and limit are pagination bounds. Offset is inclusive starting at index 0.
	// More results may exist while the number of returned items is equal to limit.
	List(pattern string, offset, limit int) ([]Task, error)
}

// Data access object for Snapshot data.
type SnapshotDAO interface {
	// Load a saved snapshot.
	// ErrNoSnapshotExists will be returned if HasSnapshot returns false.
	Get(id string) (*Snapshot, error)
	// Save a snapshot.
	Put(id string, snapshot *Snapshot) error
	// Whether a snapshot exists in the store.
	Exists(id string) (bool, error)
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
	Disabled Status = iota
	Enabled
)

type TaskType int

const (
	StreamTask TaskType = iota
	BatchTask
)

type Task struct {
	// Unique identifier for the task
	ID string
	// The task type (stream|batch).
	Type TaskType
	// The DBs and RPs the task is allowed to access.
	DBRPs []DBRP
	// The TICKscript for the task.
	TICKscript string
	// Last error the task had either while defining or executing.
	Error string
	// Status of the task
	Status Status
}

type DBRP struct {
	Database        string
	RetentionPolicy string
}

type Snapshot struct {
	NodeSnapshots map[string][]byte
}

const (
	taskDataPrefix    = "/tasks/data/"
	taskIndexesPrefix = "/tasks/indexes/"

	// Name of ID index
	idIndex = "id/"
)

// Key/Value store based implementation of the TaskDAO
type taskKV struct {
	store storage.Interface
}

func newTaskKV(store storage.Interface) *taskKV {
	return &taskKV{
		store: store,
	}
}

func (d *taskKV) encodeTask(t Task) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(t)
	return buf.Bytes(), err
}

func (d *taskKV) decodeTask(data []byte) (Task, error) {
	var task Task
	dec := gob.NewDecoder(bytes.NewReader(data))
	err := dec.Decode(&task)
	return task, err
}

// Create a key for the task data
func (d *taskKV) taskDataKey(id string) string {
	return taskDataPrefix + id
}

// Create a key for a given index and value.
//
// Indexes are maintained via a 'directory' like system:
//
// /tasks/data/ID -- contains encoded task data
// /tasks/index/id/ID -- contains the task ID
//
// As such to list all tasks in ID sorted order use the /tasks/index/id/ directory.
func (d *taskKV) taskIndexKey(index, value string) string {
	return taskIndexesPrefix + index + value
}

func (d *taskKV) Get(id string) (Task, error) {
	key := d.taskDataKey(id)
	if exists, err := d.store.Exists(key); err != nil {
		return Task{}, err
	} else if !exists {
		return Task{}, ErrNoTaskExists
	}
	kv, err := d.store.Get(key)
	if err != nil {
		return Task{}, err
	}
	return d.decodeTask(kv.Value)
}

func (d *taskKV) Create(t Task) error {
	key := d.taskDataKey(t.ID)

	exists, err := d.store.Exists(key)
	if err != nil {
		return err
	}
	if exists {
		return ErrTaskExists
	}

	data, err := d.encodeTask(t)
	if err != nil {
		return err
	}
	// Put data
	err = d.store.Put(key, data)
	if err != nil {
		return err
	}
	// Put ID index
	indexKey := d.taskIndexKey(idIndex, t.ID)
	return d.store.Put(indexKey, []byte(t.ID))
}

func (d *taskKV) Replace(t Task) error {
	key := d.taskDataKey(t.ID)

	exists, err := d.store.Exists(key)
	if err != nil {
		return err
	}
	if !exists {
		return ErrNoTaskExists
	}

	data, err := d.encodeTask(t)
	if err != nil {
		return err
	}
	// Put data
	err = d.store.Put(key, data)
	if err != nil {
		return err
	}
	return nil
}

func (d *taskKV) Delete(id string) error {
	key := d.taskDataKey(id)
	indexKey := d.taskIndexKey(idIndex, id)

	dataErr := d.store.Delete(key)
	indexErr := d.store.Delete(indexKey)
	if dataErr != nil {
		return dataErr
	}
	return indexErr
}

func (d *taskKV) List(pattern string, offset, limit int) ([]Task, error) {
	// Tasks are indexed via their ID only.
	// While tasks are sorted in the data section by their ID anyway
	// this allows us to do offset/limits and filtering without having to read in all task data.

	// List all task ids sorted by ID
	ids, err := d.store.List(taskIndexesPrefix + idIndex)
	if err != nil {
		return nil, err
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

	tasks := make([]Task, len(matches))
	for i, id := range matches {
		data, err := d.store.Get(d.taskDataKey(string(id)))
		if err != nil {
			return nil, err
		}
		t, err := d.decodeTask(data.Value)
		tasks[i] = t
	}
	return tasks, nil
}

const (
	snapshotDataPrefix = "/snapshots/data/"
)

// Key/Value implementation of SnapshotDAO
type snapshotKV struct {
	store storage.Interface
}

func newSnapshotKV(store storage.Interface) *snapshotKV {
	return &snapshotKV{
		store: store,
	}
}
func (d *snapshotKV) encodeSnapshot(snapshot *Snapshot) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(snapshot)
	return buf.Bytes(), err
}

func (d *snapshotKV) decodeSnapshot(data []byte) (*Snapshot, error) {
	snapshot := new(Snapshot)
	dec := gob.NewDecoder(bytes.NewReader(data))
	err := dec.Decode(snapshot)
	return snapshot, err
}

func (d *snapshotKV) snapshotDataKey(id string) string {
	return snapshotDataPrefix + id
}

func (d *snapshotKV) Put(id string, snapshot *Snapshot) error {
	key := d.snapshotDataKey(id)
	data, err := d.encodeSnapshot(snapshot)
	if err != nil {
		return err
	}
	return d.store.Put(key, data)
}

func (d *snapshotKV) Exists(id string) (bool, error) {
	key := d.snapshotDataKey(id)
	return d.store.Exists(key)
}

func (d *snapshotKV) Get(id string) (*Snapshot, error) {
	exists, err := d.Exists(id)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrNoSnapshotExists
	}
	key := d.snapshotDataKey(id)
	data, err := d.store.Get(key)
	if err != nil {
		return nil, err
	}
	return d.decodeSnapshot(data.Value)
}
