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
	templateDataPrefix    = "/templates/data/"
	templateIndexesPrefix = "/templates/indexes/"
	// Associate tasks with a template
	templateTaskPrefix = "/templates/tasks/"
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

func (d *templateKV) Get(id string) (Template, error) {
	key := d.templateDataKey(id)
	if exists, err := d.store.Exists(key); err != nil {
		return Template{}, err
	} else if !exists {
		return Template{}, ErrNoTemplateExists
	}
	kv, err := d.store.Get(key)
	if err != nil {
		return Template{}, err
	}
	return d.decodeTemplate(kv.Value)
}

func (d *templateKV) Create(t Template) error {
	key := d.templateDataKey(t.ID)

	exists, err := d.store.Exists(key)
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
	err = d.store.Put(key, data)
	if err != nil {
		return err
	}
	// Put ID index
	indexKey := d.templateIndexKey(idIndex, t.ID)
	return d.store.Put(indexKey, []byte(t.ID))
}

func (d *templateKV) Replace(t Template) error {
	key := d.templateDataKey(t.ID)

	exists, err := d.store.Exists(key)
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
	err = d.store.Put(key, data)
	if err != nil {
		return err
	}
	return nil
}

func (d *templateKV) Delete(id string) error {
	key := d.templateDataKey(id)
	indexKey := d.templateIndexKey(idIndex, id)

	// Try and delete everything ignore errors until after.

	dataErr := d.store.Delete(key)
	indexErr := d.store.Delete(indexKey)

	// Delete all associations
	var lastErr error
	ids, err := d.store.List(templateTaskPrefix + id + "/")
	if err != nil {
		lastErr = err
	} else {
		for _, id := range ids {
			err := d.store.Delete(id.Key)
			if err != nil {
				lastErr = err
			}
		}
	}
	if dataErr != nil {
		return dataErr
	}
	if indexErr != nil {
		return indexErr
	}
	return lastErr
}

func (d *templateKV) AssociateTask(templateId, taskId string) error {
	akey := d.templateTaskAssociationKey(templateId, taskId)
	return d.store.Put(akey, []byte(taskId))
}

func (d *templateKV) DisassociateTask(templateId, taskId string) error {
	akey := d.templateTaskAssociationKey(templateId, taskId)
	return d.store.Delete(akey)
}

func (d *templateKV) ListAssociatedTasks(templateId string) ([]string, error) {
	ids, err := d.store.List(templateTaskPrefix + templateId + "/")
	if err != nil {
		return nil, err
	}
	taskIds := make([]string, len(ids))
	for i, id := range ids {
		taskIds[i] = string(id.Value)
	}
	return taskIds, nil
}

func (d *templateKV) List(pattern string, offset, limit int) ([]Template, error) {
	// Templates are indexed via their ID only.
	// While templates are sorted in the data section by their ID anyway
	// this allows us to do offset/limits and filtering without having to read in all template data.

	// List all template ids sorted by ID
	ids, err := d.store.List(templateIndexesPrefix + idIndex)
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

	templates := make([]Template, len(matches))
	for i, id := range matches {
		data, err := d.store.Get(d.templateDataKey(string(id)))
		if err != nil {
			return nil, err
		}
		t, err := d.decodeTemplate(data.Value)
		templates[i] = t
	}
	return templates, nil
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

func (d *snapshotKV) Delete(id string) error {
	key := d.snapshotDataKey(id)
	return d.store.Delete(key)
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
