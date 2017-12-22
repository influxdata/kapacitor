package task_store

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"path"
	"time"

	"github.com/influxdata/kapacitor/services/storage"
)

var (
	ErrTaskExists       = errors.New("task already exists")
	ErrNoTaskExists     = errors.New("no task exists")
	ErrTemplateExists   = errors.New("template already exists")
	ErrNoTemplateExists = errors.New("no template exists")
	ErrNoSnapshotExists = errors.New("no snapshot exists")
)

// Data access object for Task data.
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

	Rebuild() error
}

// Data access object for Template data.
type TemplateDAO interface {
	// Retrieve a template
	Get(id string) (Template, error)

	// Create a template.
	// ErrTemplateExists is returned if a template already exists with the same ID.
	Create(t Template) error

	// Replace an existing template.
	// ErrNoTemplateExists is returned if the template does not exist.
	Replace(t Template) error

	// Delete a template.
	// It is not an error to delete an non-existent template.
	Delete(id string) error

	// List templates matching a pattern.
	// The pattern is shell/glob matching see https://golang.org/pkg/path/#Match
	// Offset and limit are pagination bounds. Offset is inclusive starting at index 0.
	// More results may exist while the number of returned items is equal to limit.
	List(pattern string, offset, limit int) ([]Template, error)

	// Associate a task with a template
	AssociateTask(templateId, taskId string) error

	// Disassociate a task with a template
	DisassociateTask(templateId, taskId string) error

	ListAssociatedTasks(templateId string) ([]string, error)
}

// Data access object for Snapshot data.
type SnapshotDAO interface {
	// Load a saved snapshot.
	// ErrNoSnapshotExists will be returned if the snapshot does not exist.
	Get(id string) (*Snapshot, error)
	// Save a snapshot.
	Put(id string, snapshot *Snapshot) error
	// Delete a snapshot
	Delete(id string) error
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
	Undefined TaskType = -1
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
	// ID of task template
	TemplateID string
	// Set of vars for a templated task
	Vars map[string]Var
	// Last error the task had either while defining or executing.
	Error string
	// Status of the task
	Status Status
	// Created Date
	Created time.Time
	// The time the task was last modified
	Modified time.Time
	// The time the task was last changed to status Enabled.
	LastEnabled time.Time
}

type rawTask Task

func (t Task) ObjectID() string {
	return t.ID
}

func (t Task) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(rawTask(t))
	return buf.Bytes(), err
}

func (t *Task) UnmarshalBinary(data []byte) error {
	dec := gob.NewDecoder(bytes.NewReader(data))
	return dec.Decode((*rawTask)(t))
}

type Template struct {
	// Unique identifier for the task
	ID string
	// The task type (stream|batch).
	Type TaskType
	// The TICKscript for the task.
	TICKscript string
	// Last error the task had either while defining or executing.
	Error string
	// Created Date
	Created time.Time
	// The time the task was last modified
	Modified time.Time
}

type DBRP struct {
	Database        string
	RetentionPolicy string
}

type Snapshot struct {
	NodeSnapshots map[string][]byte
}

// Key/Value store based implementation of the TaskDAO
type taskKV struct {
	store *storage.IndexedStore
}

func newTaskKV(store storage.Interface) (*taskKV, error) {
	c := storage.DefaultIndexedStoreConfig("tasks", func() storage.BinaryObject {
		return new(Task)
	})
	istore, err := storage.NewIndexedStore(store, c)
	if err != nil {
		return nil, err
	}
	return &taskKV{
		store: istore,
	}, nil
}

func (kv *taskKV) error(err error) error {
	if err == storage.ErrObjectExists {
		return ErrTaskExists
	} else if err == storage.ErrNoObjectExists {
		return ErrNoTaskExists
	}
	return err
}

func (kv *taskKV) Get(id string) (Task, error) {
	o, err := kv.store.Get(id)
	if err != nil {
		return Task{}, kv.error(err)
	}
	t, ok := o.(*Task)
	if !ok {
		return Task{}, fmt.Errorf("impossible error, object not a Task, got %T", o)
	}
	return *t, nil
}

func (kv *taskKV) Create(t Task) error {
	return kv.store.Create(&t)
}

func (kv *taskKV) Replace(t Task) error {
	return kv.store.Replace(&t)
}

func (kv *taskKV) Delete(id string) error {
	return kv.store.Delete(id)
}

func (kv *taskKV) List(pattern string, offset, limit int) ([]Task, error) {
	objects, err := kv.store.List(storage.DefaultIDIndex, pattern, offset, limit)
	if err != nil {
		return nil, err
	}
	tasks := make([]Task, len(objects))
	for i, o := range objects {
		t, ok := o.(*Task)
		if !ok {
			return nil, fmt.Errorf("impossible error, object not a Task, got %T", o)
		}
		tasks[i] = *t
	}
	return tasks, nil
}

func (kv *taskKV) Rebuild() error {
	return kv.store.Rebuild()
}

const (
	templateDataPrefix    = "/templates/data/"
	templateIndexesPrefix = "/templates/indexes/"
	// Associate tasks with a template
	templateTaskPrefix = "/templates/tasks/"

	idIndex = "id/"
)

// Key/Value store based implementation of the TemplateDAO
type templateKV struct {
	store storage.Interface
}

func newTemplateKV(store storage.Interface) *templateKV {
	return &templateKV{
		store: store,
	}
}

func (d *templateKV) encodeTemplate(t Template) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(t)
	return buf.Bytes(), err
}

func (d *templateKV) decodeTemplate(data []byte) (Template, error) {
	var template Template
	dec := gob.NewDecoder(bytes.NewReader(data))
	err := dec.Decode(&template)
	return template, err
}

// Create a key for the template data
func (d *templateKV) templateDataKey(id string) string {
	return templateDataPrefix + id
}

// Create a key for a given index and value.
//
// Indexes are maintained via a 'directory' like system:
//
// /templates/data/ID -- contains encoded template data
// /templates/index/id/ID -- contains the template ID
//
// As such to list all templates in ID sorted order use the /templates/index/id/ directory.
func (d *templateKV) templateIndexKey(index, value string) string {
	return templateIndexesPrefix + index + value
}

// Create a key for the template task association
func (d *templateKV) templateTaskAssociationKey(templateId, taskId string) string {
	return templateTaskPrefix + templateId + "/" + taskId
}

func (d *templateKV) Get(id string) (t Template, err error) {
	err = d.store.View(func(tx storage.ReadOnlyTx) error {
		key := d.templateDataKey(id)
		if exists, err := tx.Exists(key); err != nil {
			return err
		} else if !exists {
			return ErrNoTemplateExists
		}
		kv, err := tx.Get(key)
		if err != nil {
			return err
		}
		t, err = d.decodeTemplate(kv.Value)
		return err
	})
	return
}

func (d *templateKV) Create(t Template) error {
	return d.store.Update(func(tx storage.Tx) error {
		key := d.templateDataKey(t.ID)

		exists, err := tx.Exists(key)
		if err != nil {
			return err
		}
		if exists {
			return ErrTemplateExists
		}

		data, err := d.encodeTemplate(t)
		if err != nil {
			return err
		}
		// Put data
		err = tx.Put(key, data)
		if err != nil {
			return err
		}
		// Put ID index
		indexKey := d.templateIndexKey(idIndex, t.ID)
		return tx.Put(indexKey, []byte(t.ID))
	})
}

func (d *templateKV) Replace(t Template) error {
	return d.store.Update(func(tx storage.Tx) error {
		key := d.templateDataKey(t.ID)

		exists, err := tx.Exists(key)
		if err != nil {
			return err
		}
		if !exists {
			return ErrNoTemplateExists
		}

		data, err := d.encodeTemplate(t)
		if err != nil {
			return err
		}
		// Put data
		err = tx.Put(key, data)
		if err != nil {
			return err
		}
		return nil
	})
}

func (d *templateKV) Delete(id string) error {
	return d.store.Update(func(tx storage.Tx) error {
		key := d.templateDataKey(id)
		indexKey := d.templateIndexKey(idIndex, id)

		if err := tx.Delete(key); err != nil {
			return err
		}
		if err := tx.Delete(indexKey); err != nil {
			return err
		}

		// Delete all associations
		ids, err := tx.List(templateTaskPrefix + id + "/")
		if err != nil {
			return nil
		}

		for _, id := range ids {
			err := tx.Delete(id.Key)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (d *templateKV) AssociateTask(templateId, taskId string) error {
	return d.store.Update(func(tx storage.Tx) error {
		akey := d.templateTaskAssociationKey(templateId, taskId)
		return tx.Put(akey, []byte(taskId))
	})
}

func (d *templateKV) DisassociateTask(templateId, taskId string) error {
	return d.store.Update(func(tx storage.Tx) error {
		akey := d.templateTaskAssociationKey(templateId, taskId)
		return tx.Delete(akey)
	})
}

func (d *templateKV) ListAssociatedTasks(templateId string) (taskIds []string, err error) {
	err = d.store.View(func(tx storage.ReadOnlyTx) error {
		ids, err := tx.List(templateTaskPrefix + templateId + "/")
		if err != nil {
			return err
		}
		taskIds = make([]string, len(ids))
		for i, id := range ids {
			taskIds[i] = string(id.Value)
		}
		return nil
	})
	return
}

func (d *templateKV) List(pattern string, offset, limit int) (templates []Template, err error) {
	err = d.store.View(func(tx storage.ReadOnlyTx) error {
		// Templates are indexed via their ID only.
		// While templates are sorted in the data section by their ID anyway
		// this allows us to do offset/limits and filtering without having to read in all template data.

		// List all template ids sorted by ID
		ids, err := tx.List(templateIndexesPrefix + idIndex)
		if err != nil {
			return err
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

		templates = make([]Template, len(matches))
		for i, id := range matches {
			data, err := tx.Get(d.templateDataKey(string(id)))
			if err != nil {
				return err
			}
			t, err := d.decodeTemplate(data.Value)
			templates[i] = t
		}
		return nil
	})
	return
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
	return d.store.Update(func(tx storage.Tx) error {
		key := d.snapshotDataKey(id)
		data, err := d.encodeSnapshot(snapshot)
		if err != nil {
			return err
		}
		return tx.Put(key, data)
	})
}

func (d *snapshotKV) Delete(id string) error {
	return d.store.Update(func(tx storage.Tx) error {
		key := d.snapshotDataKey(id)
		return tx.Delete(key)
	})
}

func (d *snapshotKV) Exists(id string) (exists bool, err error) {
	err = d.store.View(func(tx storage.ReadOnlyTx) error {
		key := d.snapshotDataKey(id)
		exists, err = tx.Exists(key)
		return err
	})
	return
}

func (d *snapshotKV) Get(id string) (snap *Snapshot, err error) {
	err = d.store.View(func(tx storage.ReadOnlyTx) error {
		exists, err := d.Exists(id)
		if err != nil {
			return err
		}
		if !exists {
			return ErrNoSnapshotExists
		}
		key := d.snapshotDataKey(id)
		data, err := tx.Get(key)
		if err != nil {
			return err
		}
		snap, err = d.decodeSnapshot(data.Value)
		return err
	})
	return
}

type VarType int

const (
	VarUnknown VarType = iota
	VarBool
	VarInt
	VarFloat
	VarString
	VarRegex
	VarDuration
	VarLambda
	VarList
	VarStar
)

func (vt VarType) String() string {
	switch vt {
	case VarUnknown:
		return "unknown"
	case VarBool:
		return "bool"
	case VarInt:
		return "int"
	case VarFloat:
		return "float"
	case VarString:
		return "string"
	case VarRegex:
		return "regex"
	case VarDuration:
		return "duration"
	case VarLambda:
		return "lambda"
	case VarList:
		return "list"
	case VarStar:
		return "star"
	default:
		return "invalid"
	}
}

type Var struct {
	BoolValue     bool
	IntValue      int64
	FloatValue    float64
	StringValue   string
	RegexValue    string
	DurationValue time.Duration
	LambdaValue   string
	ListValue     []Var

	Type        VarType
	Description string
}

func newVar(value interface{}, typ VarType, desc string) (Var, error) {
	g := Var{
		Type:        typ,
		Description: desc,
	}
	if typ == VarStar {
		g.Type = VarStar
	} else {
		switch v := value.(type) {
		case bool:
			g.BoolValue = v
			g.Type = VarBool
		case int64:
			g.IntValue = v
			g.Type = VarInt
		case float64:
			g.FloatValue = v
			g.Type = VarFloat
		case time.Duration:
			g.DurationValue = v
			g.Type = VarDuration
		case string:
			switch typ {
			case VarLambda:
				g.LambdaValue = v
			case VarRegex:
				g.RegexValue = v
			case VarString:
				g.StringValue = v
			default:
				return Var{}, fmt.Errorf("unexpected var type %v, value is a string", typ)
			}
			g.Type = typ
		case []Var:
			g.ListValue = v
			g.Type = VarList
		default:
			return Var{}, fmt.Errorf("unsupported Var type %T.", value)
		}
	}
	if g.Type != typ {
		return Var{}, fmt.Errorf("mismatched type and value: %T %v", value, typ)
	}
	return g, nil
}
