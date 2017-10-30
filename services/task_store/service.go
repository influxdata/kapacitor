package task_store

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"time"

	"github.com/boltdb/bolt"
	"github.com/influxdata/kapacitor"
	"github.com/influxdata/kapacitor/client/v1"
	"github.com/influxdata/kapacitor/keyvalue"
	"github.com/influxdata/kapacitor/server/vars"
	"github.com/influxdata/kapacitor/services/httpd"
	"github.com/influxdata/kapacitor/services/storage"
	"github.com/influxdata/kapacitor/tick"
	"github.com/influxdata/kapacitor/tick/ast"
	"github.com/influxdata/kapacitor/uuid"
	"github.com/pkg/errors"
)

const (
	tasksPath         = "/tasks"
	tasksPathAnchored = "/tasks/"

	templatesPath         = "/templates"
	templatesPathAnchored = "/templates/"
)

type Diagnostic interface {
	StartingTask(taskID string)
	StartedTask(taskID string)

	FinishedTask(taskID string)

	Error(msg string, err error, ctx ...keyvalue.T)

	Debug(msg string)

	AlreadyMigrated(entity, id string)
	Migrated(entity, id string)
}

type Service struct {
	oldDBDir         string
	tasks            TaskDAO
	templates        TemplateDAO
	snapshots        SnapshotDAO
	routes           []httpd.Route
	snapshotInterval time.Duration
	StorageService   interface {
		Store(namespace string) storage.Interface
		Register(name string, store storage.StoreActioner)
	}
	HTTPDService interface {
		AddRoutes([]httpd.Route) error
		DelRoutes([]httpd.Route)
	}
	TaskMasterLookup interface {
		Main() *kapacitor.TaskMaster
		Get(string) *kapacitor.TaskMaster
		Set(*kapacitor.TaskMaster)
		Delete(*kapacitor.TaskMaster)
	}

	diag Diagnostic
}

type taskStore struct {
	Name       string
	Type       kapacitor.TaskType
	TICKScript string
}

func NewService(conf Config, d Diagnostic) *Service {
	return &Service{
		snapshotInterval: time.Duration(conf.SnapshotInterval),
		diag:             d,
		oldDBDir:         conf.Dir,
	}
}

const (
	// Public name for the task storage layer
	tasksAPIName = "tasks"
	// The storage namespace for all task data.
	taskNamespace = "task_store"
)

func (ts *Service) Open() error {
	// Create DAO
	store := ts.StorageService.Store(taskNamespace)
	tasksDAO, err := newTaskKV(store)
	if err != nil {
		return err
	}
	ts.tasks = tasksDAO
	ts.StorageService.Register(tasksAPIName, ts.tasks)
	ts.templates = newTemplateKV(store)
	ts.snapshots = newSnapshotKV(store)

	// Perform migration to new storage service.
	if err := ts.migrate(); err != nil {
		return err
	}

	// Define API routes
	ts.routes = []httpd.Route{
		{
			Method:      "GET",
			Pattern:     tasksPathAnchored,
			HandlerFunc: ts.handleTask,
		},
		{
			Method:      "DELETE",
			Pattern:     tasksPathAnchored,
			HandlerFunc: ts.handleDeleteTask,
		},
		{
			// Satisfy CORS checks.
			Method:      "OPTIONS",
			Pattern:     tasksPathAnchored,
			HandlerFunc: httpd.ServeOptions,
		},
		{
			Method:      "PATCH",
			Pattern:     tasksPathAnchored,
			HandlerFunc: ts.handleUpdateTask,
		},
		{
			Method:      "GET",
			Pattern:     tasksPath,
			HandlerFunc: ts.handleListTasks,
		},
		{
			Method:      "POST",
			Pattern:     tasksPath,
			HandlerFunc: ts.handleCreateTask,
		},
		{
			Method:      "GET",
			Pattern:     templatesPathAnchored,
			HandlerFunc: ts.handleTemplate,
		},
		{
			Method:      "DELETE",
			Pattern:     templatesPathAnchored,
			HandlerFunc: ts.handleDeleteTemplate,
		},
		{
			// Satisfy CORS checks.
			Method:      "OPTIONS",
			Pattern:     templatesPathAnchored,
			HandlerFunc: httpd.ServeOptions,
		},
		{
			Method:      "PATCH",
			Pattern:     templatesPathAnchored,
			HandlerFunc: ts.handleUpdateTemplate,
		},
		{
			Method:      "GET",
			Pattern:     templatesPath,
			HandlerFunc: ts.handleListTemplates,
		},
		{
			Method:      "POST",
			Pattern:     templatesPath,
			HandlerFunc: ts.handleCreateTemplate,
		},
	}

	err = ts.HTTPDService.AddRoutes(ts.routes)
	if err != nil {
		return err
	}

	numTasks := int64(0)
	numEnabledTasks := int64(0)

	// Count all tasks
	offset := 0
	limit := 100
	vars.NumEnabledTasksVar.Set(0)
	for {
		tasks, err := ts.tasks.List("*", offset, limit)
		if err != nil {
			return err
		}
		for _, task := range tasks {
			numTasks++
			if task.Status == Enabled {
				numEnabledTasks++
				ts.diag.StartingTask(task.ID)
				err = ts.startTask(task)
				if err != nil {
					ts.diag.Error("failed to start enabled task", err, keyvalue.KV("task", task.ID))
				} else {
					ts.diag.StartedTask(task.ID)
				}
			}
		}
		if len(tasks) != limit {
			break
		}
		offset += limit
	}

	// Set expvars
	vars.NumTasksVar.Set(numTasks)
	vars.NumEnabledTasksVar.Set(numEnabledTasks)

	return nil
}

// Migrate data from previous task.db to new storage service.
// This process will return any errors and stop the TaskStore from opening
// thus stopping the entire Kapacitor startup.
// This way Kapacitor will not be able to startup without successfully migrating
// to the new scheme.
//
// This process is idempotent and can be attempted multiple times until success is achieved.
func (ts *Service) migrate() error {
	if ts.oldDBDir == "" {
		return nil
	}

	tasksBucket := []byte("tasks")
	enabledBucket := []byte("enabled")
	snapshotBucket := []byte("snapshots")

	// Connect to old boltdb
	db, err := bolt.Open(filepath.Join(ts.oldDBDir, "task.db"), 0600, &bolt.Options{ReadOnly: true})
	if err != nil {
		ts.diag.Debug("could not open old boltd for task_store. Not performing migration. Remove the `task_store.dir` configuration to disable migration.")
		return nil
	}

	// Old task format
	type rawTask struct {
		// The name of the task.
		Name string
		// The TICKscript for the task.
		TICKscript string
		// Last error the task had either while defining or executing.
		Error string
		// The task type (stream|batch).
		Type kapacitor.TaskType
		// The DBs and RPs the task is allowed to access.
		DBRPs            []kapacitor.DBRP
		SnapshotInterval time.Duration
	}

	// Migrate all tasks
	err = db.View(func(tx *bolt.Tx) error {
		tasks := tx.Bucket([]byte(tasksBucket))
		if tasks == nil {
			return nil
		}
		enables := tx.Bucket([]byte(enabledBucket))
		return tasks.ForEach(func(k, v []byte) error {
			r := bytes.NewReader(v)
			dec := gob.NewDecoder(r)
			task := &rawTask{}
			err = dec.Decode(task)
			if err != nil {
				ts.diag.Error("corrupt data in old task_store boltdb tasks", err)
				return nil
			}

			var typ TaskType
			switch task.Type {
			case kapacitor.StreamTask:
				typ = StreamTask
			case kapacitor.BatchTask:
				typ = BatchTask
			}

			dbrps := make([]DBRP, len(task.DBRPs))
			for i, dbrp := range task.DBRPs {
				dbrps[i] = DBRP{
					Database:        dbrp.Database,
					RetentionPolicy: dbrp.RetentionPolicy,
				}
			}

			status := Disabled
			if enables != nil {
				data := enables.Get(k)
				if data != nil {
					status = Enabled
				}
			}

			now := time.Now()
			newTask := Task{
				ID:         task.Name,
				Type:       typ,
				DBRPs:      dbrps,
				TICKscript: task.TICKscript,
				Error:      task.Error,
				Status:     status,
				Created:    now,
				Modified:   now,
			}
			if newTask.Status == Enabled {
				newTask.LastEnabled = now
			}
			// Try and create the task in the new store.
			err = ts.tasks.Create(newTask)
			if err != nil {
				if err != ErrTaskExists {
					// Failed to migrate task stop process
					return err
				} else {
					ts.diag.AlreadyMigrated("task", task.Name)
				}
			} else {
				ts.diag.Migrated("task", task.Name)
			}
			return nil
		})
	})
	if err != nil {
		return errors.Wrap(err, "migrating tasks")
	}

	// Migrate all snapshots
	err = db.View(func(tx *bolt.Tx) error {
		snapshots := tx.Bucket([]byte(snapshotBucket))
		if snapshots == nil {
			return nil
		}
		return snapshots.ForEach(func(k, v []byte) error {
			r := bytes.NewReader(v)
			dec := gob.NewDecoder(r)
			snapshot := &kapacitor.TaskSnapshot{}
			err = dec.Decode(snapshot)
			if err != nil {
				ts.diag.Error("corrupt data in old task_store boltdb snapshots", err)
				return nil
			}

			newSnapshot := &Snapshot{
				NodeSnapshots: snapshot.NodeSnapshots,
			}
			id := string(k)
			if exists, err := ts.snapshots.Exists(id); err == nil {
				if !exists {
					err := ts.snapshots.Put(string(k), newSnapshot)
					if err != nil {
						// Failed to migrate snapshot stop process.
						return err
					}
					ts.diag.Migrated("snapshot", id)
				} else {
					ts.diag.AlreadyMigrated("snapshot", id)
				}
			} else if err != nil {
				return err
			}
			return nil
		})
	})
	if err != nil {
		return errors.Wrap(err, "migrating snapshots")
	}
	return nil
}

func (ts *Service) Close() error {
	ts.HTTPDService.DelRoutes(ts.routes)
	return nil
}

func (ts *Service) Load(id string) (*kapacitor.Task, error) {
	t, err := ts.tasks.Get(id)
	if err != nil {
		return nil, err
	}
	return ts.newKapacitorTask(t)
}

func (ts *Service) SaveSnapshot(id string, snapshot *kapacitor.TaskSnapshot) error {
	s := &Snapshot{
		NodeSnapshots: snapshot.NodeSnapshots,
	}
	return ts.snapshots.Put(id, s)
}

func (ts *Service) HasSnapshot(id string) bool {
	exists, err := ts.snapshots.Exists(id)
	if err != nil {
		ts.diag.Error("error checking for snapshot", err)
		return false
	}
	return exists
}

func (ts *Service) LoadSnapshot(id string) (*kapacitor.TaskSnapshot, error) {
	snapshot, err := ts.snapshots.Get(id)
	if err != nil {
		return nil, err
	}
	s := &kapacitor.TaskSnapshot{
		NodeSnapshots: snapshot.NodeSnapshots,
	}
	return s, nil
}

type TaskInfo struct {
	Name           string
	Type           kapacitor.TaskType
	DBRPs          []DBRP
	TICKscript     string
	Dot            string
	Enabled        bool
	Executing      bool
	Error          string
	ExecutionStats kapacitor.ExecutionStats
}

func (ts *Service) handleTask(w http.ResponseWriter, r *http.Request) {
	id, err := ts.taskIDFromPath(r.URL.Path)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusBadRequest)
		return
	}

	raw, err := ts.tasks.Get(id)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusNotFound)
		return
	}

	scriptFormat := r.URL.Query().Get("script-format")
	switch scriptFormat {
	case "", "formatted":
		scriptFormat = "formatted"
	case "raw":
	default:
		httpd.HttpError(w, fmt.Sprintf("invalid script-format parameter %q", scriptFormat), true, http.StatusBadRequest)
		return
	}

	dotView := r.URL.Query().Get("dot-view")
	switch dotView {
	case "":
		dotView = "attributes"
	case "attributes":
	case "labels":
	default:
		httpd.HttpError(w, fmt.Sprintf("invalid dot-view parameter %q", dotView), true, http.StatusBadRequest)
		return
	}

	tmID := r.URL.Query().Get("replay-id")
	if tmID == "" {
		tmID = kapacitor.MainTaskMaster
	}

	tm := ts.TaskMasterLookup.Get(tmID)

	if tm == nil {
		httpd.HttpError(w, fmt.Sprintf("no running replay with ID: %s", tmID), true, http.StatusBadRequest)
		return
	}
	if tmID != kapacitor.MainTaskMaster && !tm.IsExecuting(raw.ID) {
		httpd.HttpError(w, fmt.Sprintf("replay %s is not for task: %s", tmID, raw.ID), true, http.StatusBadRequest)
		return
	}
	t, err := ts.convertTask(raw, scriptFormat, dotView, tm)
	if err != nil {
		httpd.HttpError(w, fmt.Sprintf("invalid task stored in db: %s", err.Error()), true, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(httpd.MarshalJSON(t, true))
}

var allTaskFields = []string{
	"link",
	"id",
	"type",
	"dbrps",
	"script",
	"dot",
	"status",
	"executing",
	"error",
	"stats",
	"created",
	"modified",
	"last-enabled",
	"vars",
}

const tasksBasePathAnchored = httpd.BasePath + tasksPathAnchored

func (ts *Service) taskIDFromPath(path string) (string, error) {
	if len(path) <= len(tasksBasePathAnchored) {
		return "", errors.New("must specify task id on path")
	}
	id := path[len(tasksBasePathAnchored):]
	return id, nil
}

func (ts *Service) taskLink(id string) client.Link {
	return client.Link{Relation: client.Self, Href: path.Join(httpd.BasePath, tasksPath, id)}
}

func (ts *Service) handleListTasks(w http.ResponseWriter, r *http.Request) {

	pattern := r.URL.Query().Get("pattern")
	fields := r.URL.Query()["fields"]
	if len(fields) == 0 {
		fields = allTaskFields
	} else {
		// Always return ID field
		fields = append(fields, "id", "link")
	}

	scriptFormat := r.URL.Query().Get("script-format")
	switch scriptFormat {
	case "":
		scriptFormat = "formatted"
	case "formatted":
	case "raw":
	default:
		httpd.HttpError(w, fmt.Sprintf("invalid script-format parameter %q", scriptFormat), true, http.StatusBadRequest)
		return
	}

	dotView := r.URL.Query().Get("dot-view")
	switch dotView {
	case "":
		dotView = "attributes"
	case "attributes":
	case "labels":
	default:
		httpd.HttpError(w, fmt.Sprintf("invalid dot-view parameter %q", dotView), true, http.StatusBadRequest)
		return
	}

	var err error
	offset := int64(0)
	offsetStr := r.URL.Query().Get("offset")
	if offsetStr != "" {
		offset, err = strconv.ParseInt(offsetStr, 10, 64)
		if err != nil {
			httpd.HttpError(w, fmt.Sprintf("invalid offset parameter %q must be an integer: %s", offsetStr, err), true, http.StatusBadRequest)
			return
		}
	}

	limit := int64(100)
	limitStr := r.URL.Query().Get("limit")
	if limitStr != "" {
		limit, err = strconv.ParseInt(limitStr, 10, 64)
		if err != nil {
			httpd.HttpError(w, fmt.Sprintf("invalid limit parameter %q must be an integer: %s", limitStr, err), true, http.StatusBadRequest)
			return
		}
	}

	rawTasks, err := ts.tasks.List(pattern, int(offset), int(limit))
	if err != nil {
		httpd.HttpError(w, fmt.Sprintf("failed to list tasks with pattern %q: %s", pattern, err), true, http.StatusBadRequest)
		return
	}
	tasks := make([]map[string]interface{}, len(rawTasks))

	tm := ts.TaskMasterLookup.Main()

	for i, task := range rawTasks {
		tasks[i] = make(map[string]interface{}, len(fields))
		executing := tm.IsExecuting(task.ID)
		for _, field := range fields {
			var value interface{}
			switch field {
			case "id":
				value = task.ID
			case "link":
				value = ts.taskLink(task.ID)
			case "type":
				switch task.Type {
				case StreamTask:
					value = client.StreamTask
				case BatchTask:
					value = client.BatchTask
				}
			case "dbrps":
				dbrps := make([]client.DBRP, len(task.DBRPs))
				for i, dbrp := range task.DBRPs {
					dbrps[i] = client.DBRP{
						Database:        dbrp.Database,
						RetentionPolicy: dbrp.RetentionPolicy,
					}
				}
				value = dbrps
			case "script":
				value = task.TICKscript
				if scriptFormat == "formatted" {
					formatted, err := tick.Format(task.TICKscript)
					if err == nil {
						// Only format if it succeeded.
						// Otherwise a change in syntax may prevent task retrieval.
						value = formatted
					}
				}
			case "executing":
				value = executing
			case "dot":
				if executing {
					value = tm.ExecutingDot(task.ID, dotView == "labels")
				} else {
					kt, err := ts.newKapacitorTask(task)
					if err != nil {
						break
					}
					value = string(kt.Dot())
				}
			case "stats":
				if executing {
					s, err := tm.ExecutionStats(task.ID)
					if err != nil {
						ts.diag.Error("failed to retriete stats for task", err, keyvalue.KV("task", task.ID))
					} else {
						value = client.ExecutionStats{
							TaskStats: s.TaskStats,
							NodeStats: s.NodeStats,
						}
					}
				}
			case "error":
				value = task.Error
			case "status":
				switch task.Status {
				case Disabled:
					value = client.Disabled
				case Enabled:
					value = client.Enabled
				}
			case "created":
				value = task.Created
			case "modified":
				value = task.Modified
			case "last-enabled":
				value = task.LastEnabled
			case "vars":
				vars, err := ts.convertToClientVars(task.Vars)
				if err != nil {
					ts.diag.Error("failed to get vars for task", err, keyvalue.KV("task", task.ID))
					break
				}
				value = vars
			default:
				httpd.HttpError(w, fmt.Sprintf("unsupported field %q", field), true, http.StatusBadRequest)
				return
			}
			tasks[i][field] = value
		}
	}

	type response struct {
		Tasks []map[string]interface{} `json:"tasks"`
	}

	w.Write(httpd.MarshalJSON(response{tasks}, true))
}

var validTaskID = regexp.MustCompile(`^[-\._\p{L}0-9]+$`)

func (ts *Service) handleCreateTask(w http.ResponseWriter, r *http.Request) {
	task := client.CreateTaskOptions{}
	dec := json.NewDecoder(r.Body)
	err := dec.Decode(&task)
	if err != nil {
		httpd.HttpError(w, "invalid JSON", true, http.StatusBadRequest)
		return
	}
	if task.ID == "" {
		task.ID = uuid.New().String()
	}
	if !validTaskID.MatchString(task.ID) {
		httpd.HttpError(w, fmt.Sprintf("task ID must contain only letters, numbers, '-', '.' and '_'. %q", task.ID), true, http.StatusBadRequest)
		return
	}

	newTask := Task{
		ID: task.ID,
	}

	// Check for existing task
	_, err = ts.tasks.Get(task.ID)
	if err == nil {
		httpd.HttpError(w, fmt.Sprintf("task %s already exists", task.ID), true, http.StatusBadRequest)
		return
	}

	// Check for template ID
	if task.TemplateID != "" {
		template, err := ts.templates.Get(task.TemplateID)
		if err != nil {
			httpd.HttpError(w, fmt.Sprintf("unknown template %s: err: %s", task.TemplateID, err), true, http.StatusBadRequest)
			return
		}
		newTask.Type = template.Type
		newTask.TICKscript = template.TICKscript
		newTask.TemplateID = task.TemplateID
		switch template.Type {
		case StreamTask:
			task.Type = client.StreamTask
		case BatchTask:
			task.Type = client.BatchTask
		}
		task.TICKscript = template.TICKscript
		if err := ts.templates.AssociateTask(task.TemplateID, newTask.ID); err != nil {
			httpd.HttpError(w, fmt.Sprintf("failed to associate task with template: %s", err), true, http.StatusBadRequest)
			return
		}
	} else {
		// Set task type
		switch task.Type {
		case client.StreamTask:
			newTask.Type = StreamTask
		case client.BatchTask:
			newTask.Type = BatchTask
		}

		// Set tick script
		newTask.TICKscript = task.TICKscript
		if newTask.TICKscript == "" {
			httpd.HttpError(w, fmt.Sprintf("must provide TICKscript"), true, http.StatusBadRequest)
			return
		}
	}

	// Set dbrps
	newTask.DBRPs = make([]DBRP, len(task.DBRPs))
	for i, dbrp := range task.DBRPs {
		newTask.DBRPs[i] = DBRP{
			Database:        dbrp.Database,
			RetentionPolicy: dbrp.RetentionPolicy,
		}
	}

	// Set status
	switch task.Status {
	case client.Enabled:
		newTask.Status = Enabled
	case client.Disabled:
		newTask.Status = Disabled
	default:
		newTask.Status = Disabled
	}

	// Set vars
	newTask.Vars, err = ts.convertToServiceVars(task.Vars)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusBadRequest)
		return
	}
	// Check for parity between tickscript and dbrp

	pn, err := newProgramNodeFromTickscript(newTask.TICKscript)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusBadRequest)
		return
	}

	switch tt := taskTypeFromProgram(pn); tt {
	case client.StreamTask:
		newTask.Type = StreamTask
	case client.BatchTask:
		newTask.Type = BatchTask
	default:
		httpd.HttpError(w, fmt.Sprintf("invalid task type: %v", tt), true, http.StatusBadRequest)
		return
	}

	// Validate task
	_, err = ts.newKapacitorTask(newTask)
	if err != nil {
		httpd.HttpError(w, "invalid TICKscript: "+err.Error(), true, http.StatusBadRequest)
		return
	}

	now := time.Now()
	newTask.Created = now
	newTask.Modified = now
	if newTask.Status == Enabled {
		newTask.LastEnabled = now
	}

	dbrps := []DBRP{}
	for _, dbrp := range dbrpsFromProgram(pn) {
		dbrps = append(dbrps, DBRP{
			Database:        dbrp.Database,
			RetentionPolicy: dbrp.RetentionPolicy,
		})
	}

	if len(dbrps) == 0 && len(newTask.DBRPs) == 0 {
		httpd.HttpError(w, "must specify dbrp", true, http.StatusBadRequest)
		return
	}

	if len(dbrps) > 0 && len(newTask.DBRPs) > 0 {
		httpd.HttpError(w, "cannot specify dbrp in both implicitly and explicitly", true, http.StatusBadRequest)
		return
	}

	if len(dbrps) != 0 {
		newTask.DBRPs = dbrps
	}

	// Save task
	err = ts.tasks.Create(newTask)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusInternalServerError)
		return
	}

	// Count new task
	vars.NumTasksVar.Add(1)
	if newTask.Status == Enabled {
		//Count new enabled task
		vars.NumEnabledTasksVar.Add(1)
		// Start task
		err = ts.startTask(newTask)
		if err != nil {
			httpd.HttpError(w, err.Error(), true, http.StatusInternalServerError)
			return
		}
	}

	// Return task info
	t, err := ts.convertTask(newTask, "formatted", "attributes", ts.TaskMasterLookup.Main())
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(httpd.MarshalJSON(t, true))
}

func (ts *Service) handleUpdateTask(w http.ResponseWriter, r *http.Request) {
	id, err := ts.taskIDFromPath(r.URL.Path)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusBadRequest)
		return
	}
	task := client.UpdateTaskOptions{}
	dec := json.NewDecoder(r.Body)
	err = dec.Decode(&task)
	if err != nil {
		httpd.HttpError(w, "invalid JSON", true, http.StatusBadRequest)
		return
	}

	// Check for existing task
	original, err := ts.tasks.Get(id)
	if err != nil {
		httpd.HttpError(w, "task does not exist, cannot update", true, http.StatusNotFound)
		return
	}
	updated := original

	// Set ID if changing
	if task.ID != "" {
		updated.ID = task.ID
	}

	if task.TemplateID != "" || updated.TemplateID != "" {
		templateID := task.TemplateID
		if templateID == "" {
			templateID = updated.TemplateID
		}
		template, err := ts.templates.Get(templateID)
		if err != nil {
			httpd.HttpError(w, fmt.Sprintf("unknown template %s: err: %s", task.TemplateID, err), true, http.StatusBadRequest)
			return
		}
		if original.ID != updated.ID || original.TemplateID != updated.TemplateID {
			if original.TemplateID != "" {
				if err := ts.templates.DisassociateTask(original.TemplateID, original.ID); err != nil {
					httpd.HttpError(w, fmt.Sprintf("failed to disassociate task with template: %s", err), true, http.StatusBadRequest)
					return
				}
			}
			if err := ts.templates.AssociateTask(templateID, updated.ID); err != nil {
				httpd.HttpError(w, fmt.Sprintf("failed to associate task with template: %s", err), true, http.StatusBadRequest)
				return
			}
		}
		updated.Type = template.Type
		updated.TICKscript = template.TICKscript
		updated.TemplateID = templateID
	} else {
		// Only set type and script if not a templated task
		// Set task type
		switch task.Type {
		case client.StreamTask:
			updated.Type = StreamTask
		case client.BatchTask:
			updated.Type = BatchTask
		}

		oldPn, err := newProgramNodeFromTickscript(updated.TICKscript)
		if err != nil {
			httpd.HttpError(w, err.Error(), true, http.StatusBadRequest)
			return
		}

		// Set tick script
		if task.TICKscript != "" {
			updated.TICKscript = task.TICKscript

			newPn, err := newProgramNodeFromTickscript(updated.TICKscript)
			if err != nil {
				httpd.HttpError(w, err.Error(), true, http.StatusBadRequest)
				return
			}

			if len(dbrpsFromProgram(oldPn)) > 0 && len(dbrpsFromProgram(newPn)) == 0 && len(task.DBRPs) == 0 {
				httpd.HttpError(w, "must specify dbrp", true, http.StatusBadRequest)
				return
			}
		}
	}

	pn, err := newProgramNodeFromTickscript(updated.TICKscript)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusBadRequest)
		return
	}

	if dbrps := dbrpsFromProgram(pn); len(dbrps) > 0 && len(task.DBRPs) > 0 {
		httpd.HttpError(w, "cannot specify dbrp in implicitly and explicitly", true, http.StatusBadRequest)
		return
	} else if len(dbrps) > 0 {
		// make consistent
		updated.DBRPs = []DBRP{}
		for _, dbrp := range dbrpsFromProgram(pn) {
			updated.DBRPs = append(updated.DBRPs, DBRP{
				Database:        dbrp.Database,
				RetentionPolicy: dbrp.RetentionPolicy,
			})
		}
	} else if len(task.DBRPs) > 0 {
		updated.DBRPs = make([]DBRP, len(task.DBRPs))
		for i, dbrp := range task.DBRPs {
			updated.DBRPs[i] = DBRP{
				Database:        dbrp.Database,
				RetentionPolicy: dbrp.RetentionPolicy,
			}
		}
	}

	// Set status
	previousStatus := updated.Status
	switch task.Status {
	case client.Enabled:
		updated.Status = Enabled
	case client.Disabled:
		updated.Status = Disabled
	}
	statusChanged := previousStatus != updated.Status

	// Set vars
	if len(task.Vars) > 0 {
		updated.Vars, err = ts.convertToServiceVars(task.Vars)
		if err != nil {
			httpd.HttpError(w, err.Error(), true, http.StatusBadRequest)
			return
		}
	}

	// set task type from tickscript
	switch tt := taskTypeFromProgram(pn); tt {
	case client.StreamTask:
		updated.Type = StreamTask
	case client.BatchTask:
		updated.Type = BatchTask
	default:
		httpd.HttpError(w, fmt.Sprintf("invalid task type: %v", tt), true, http.StatusBadRequest)
		return
	}

	// Validate task
	_, err = ts.newKapacitorTask(updated)
	if err != nil {
		httpd.HttpError(w, "invalid TICKscript: "+err.Error(), true, http.StatusBadRequest)
		return
	}

	now := time.Now()
	updated.Modified = now
	if statusChanged && updated.Status == Enabled {
		updated.LastEnabled = now
	}

	if original.ID != updated.ID {
		// Task ID changed delete and re-create.
		if err := ts.tasks.Create(updated); err != nil {
			httpd.HttpError(w, fmt.Sprintf("failed to create new task during ID change: %s", err.Error()), true, http.StatusInternalServerError)
			return
		}
		if err := ts.tasks.Delete(original.ID); err != nil {
			ts.diag.Error(
				"failed to delete old task definition during ID change",
				err,
				keyvalue.KV("oldID", original.ID),
				keyvalue.KV("newID", updated.ID),
			)
		}
		if original.Status == Enabled && updated.Status == Enabled {
			// Stop task and start it under new name
			ts.stopTask(original.ID)
			if err := ts.startTask(updated); err != nil {
				httpd.HttpError(w, err.Error(), true, http.StatusInternalServerError)
				return
			}
		}
	} else {
		if err := ts.tasks.Replace(updated); err != nil {
			httpd.HttpError(w, fmt.Sprintf("failed to replace task definition: %s", err.Error()), true, http.StatusInternalServerError)
			return
		}
	}

	if statusChanged {
		// Enable/Disable task
		switch updated.Status {
		case Enabled:
			vars.NumEnabledTasksVar.Add(1)
			err = ts.startTask(updated)
			if err != nil {
				httpd.HttpError(w, err.Error(), true, http.StatusInternalServerError)
				return
			}
		case Disabled:
			vars.NumEnabledTasksVar.Add(-1)
			ts.stopTask(original.ID)
		}
	}

	t, err := ts.convertTask(updated, "formatted", "attributes", ts.TaskMasterLookup.Main())
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(httpd.MarshalJSON(t, true))
}

func (ts *Service) convertTask(t Task, scriptFormat, dotView string, tm *kapacitor.TaskMaster) (client.Task, error) {
	script := t.TICKscript
	if scriptFormat == "formatted" {
		// Format TICKscript
		formatted, err := tick.Format(script)
		if err == nil {
			// Only format if it succeeded.
			// Otherwise a change in syntax may prevent task retrieval.
			script = formatted
		}
	}

	executing := tm.IsExecuting(t.ID)
	errMsg := t.Error
	dot := ""
	stats := client.ExecutionStats{}
	task, err := ts.newKapacitorTask(t)
	if err == nil {
		if executing {
			dot = tm.ExecutingDot(t.ID, dotView == "labels")
			s, err := tm.ExecutionStats(t.ID)
			if err != nil {
				ts.diag.Error("failed to retrieve stats for task", err, keyvalue.KV("task", t.ID))
			} else {
				stats.TaskStats = s.TaskStats
				stats.NodeStats = s.NodeStats
			}
		} else {
			dot = string(task.Dot())
		}
	} else {
		errMsg = err.Error()
	}

	var status client.TaskStatus
	switch t.Status {
	case Disabled:
		status = client.Disabled
	case Enabled:
		status = client.Enabled
	default:
		return client.Task{}, fmt.Errorf("invalid task status %v", t.Status)
	}

	var typ client.TaskType
	switch t.Type {
	case StreamTask:
		typ = client.StreamTask
	case BatchTask:
		typ = client.BatchTask
	default:
		return client.Task{}, fmt.Errorf("invalid task type %v", t.Type)
	}

	dbrps := make([]client.DBRP, len(t.DBRPs))
	for i, dbrp := range t.DBRPs {
		dbrps[i] = client.DBRP{
			Database:        dbrp.Database,
			RetentionPolicy: dbrp.RetentionPolicy,
		}
	}

	vars, err := ts.convertToClientVars(t.Vars)
	if err != nil {
		return client.Task{}, err
	}

	return client.Task{
		Link:           ts.taskLink(t.ID),
		ID:             t.ID,
		TemplateID:     t.TemplateID,
		Type:           typ,
		DBRPs:          dbrps,
		TICKscript:     script,
		Vars:           vars,
		Status:         status,
		Dot:            dot,
		Executing:      executing,
		ExecutionStats: stats,
		Created:        t.Created,
		Modified:       t.Modified,
		LastEnabled:    t.LastEnabled,
		Error:          errMsg,
	}, nil
}

func (ts *Service) convertToServiceVar(cvar client.Var) (Var, error) {
	v := cvar.Value
	var typ VarType
	switch cvar.Type {
	case client.VarBool:
		typ = VarBool
	case client.VarInt:
		typ = VarInt
	case client.VarFloat:
		typ = VarFloat
	case client.VarString:
		typ = VarString
	case client.VarRegex:
		typ = VarRegex
	case client.VarDuration:
		typ = VarDuration
	case client.VarLambda:
		typ = VarLambda
	case client.VarList:
		typ = VarList
		values, ok := cvar.Value.([]client.Var)
		if !ok {
			return Var{}, fmt.Errorf("var has list type but value is not list, got %T", cvar.Value)
		}
		vars := make([]Var, len(values))
		var err error
		for i := range values {
			vars[i], err = ts.convertToServiceVar(values[i])
			if err != nil {
				return Var{}, err
			}
		}
		v = vars
	case client.VarStar:
		typ = VarStar
	}
	return newVar(v, typ, cvar.Description)
}

func (ts *Service) convertToServiceVars(cvars client.Vars) (map[string]Var, error) {
	vars := make(map[string]Var, len(cvars))
	for name, value := range cvars {
		v, err := ts.convertToServiceVar(value)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid var %s", name)
		}
		vars[name] = v
	}
	return vars, nil
}

func (ts *Service) convertToClientVar(svar Var) (client.Var, error) {
	var v interface{}
	var typ client.VarType
	switch svar.Type {
	case VarBool:
		v = svar.BoolValue
		typ = client.VarBool
	case VarInt:
		v = svar.IntValue
		typ = client.VarInt
	case VarFloat:
		v = svar.FloatValue
		typ = client.VarFloat
	case VarDuration:
		v = svar.DurationValue
		typ = client.VarDuration
	case VarLambda:
		v = svar.LambdaValue
		typ = client.VarLambda
	case VarString:
		v = svar.StringValue
		typ = client.VarString
	case VarRegex:
		v = svar.RegexValue
		typ = client.VarRegex
	case VarStar:
		typ = client.VarStar
	case VarList:
		values := make([]client.Var, len(svar.ListValue))
		var err error
		for i := range svar.ListValue {
			values[i], err = ts.convertToClientVar(svar.ListValue[i])
			if err != nil {
				return client.Var{}, err
			}
		}
		v = values
		typ = client.VarList
	default:
		return client.Var{}, fmt.Errorf("unknown var: %v", svar)
	}
	return client.Var{
		Value:       v,
		Type:        typ,
		Description: svar.Description,
	}, nil
}

func (ts *Service) convertToClientVars(svars map[string]Var) (client.Vars, error) {
	vars := make(client.Vars, len(svars))
	for name, value := range svars {
		v, err := ts.convertToClientVar(value)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid var %s", name)
		}
		vars[name] = v
	}
	return vars, nil
}

func (ts *Service) convertToClientVarFromTick(kvar tick.Var) (client.Var, error) {
	v := kvar.Value
	var typ client.VarType
	switch kvar.Type {
	case ast.TBool:
		typ = client.VarBool
	case ast.TInt:
		typ = client.VarInt
	case ast.TFloat:
		typ = client.VarFloat
	case ast.TDuration:
		typ = client.VarDuration
	case ast.TStar:
		typ = client.VarStar
	case ast.TLambda:
		typ = client.VarLambda
		if l, ok := v.(*ast.LambdaNode); ok {
			v = l.ExpressionString()
		} else if v != nil {
			return client.Var{}, fmt.Errorf("invalid lambda value type, expected: *ast.LambdaNode, got %T", v)
		}
	case ast.TString:
		typ = client.VarString
	case ast.TRegex:
		typ = client.VarRegex
		if r, ok := v.(*regexp.Regexp); ok {
			v = r.String()
		} else if v != nil {
			return client.Var{}, fmt.Errorf("invalid regex value type, expected: *regexp.Regexp, got %T", v)
		}
	case ast.TList:
		typ = client.VarList
		if kvar.Value != nil {

			list, ok := kvar.Value.([]tick.Var)
			if !ok {
				return client.Var{}, fmt.Errorf("invalid list value type, expected: %T, got: %T", list, v)
			}
			values := make([]client.Var, len(list))
			var err error
			for i := range list {
				values[i], err = ts.convertToClientVarFromTick(list[i])
				if err != nil {
					return client.Var{}, err
				}
			}
			v = values
		}
	default:
		return client.Var{}, fmt.Errorf("unkown var: %v", kvar)
	}
	return client.Var{
		Value:       v,
		Type:        typ,
		Description: kvar.Description,
	}, nil
}

func (ts *Service) convertToClientVarsFromTick(kvars map[string]tick.Var) (client.Vars, error) {
	vars := make(client.Vars, len(kvars))
	for name, value := range kvars {
		v, err := ts.convertToClientVarFromTick(value)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid var %s", name)
		}
		vars[name] = v
	}
	return vars, nil
}
func (ts *Service) convertToTickVarFromService(svar Var) (tick.Var, error) {
	var v interface{}
	var typ ast.ValueType
	switch svar.Type {
	case VarBool:
		typ = ast.TBool
		v = svar.BoolValue
	case VarInt:
		typ = ast.TInt
		v = svar.IntValue
	case VarFloat:
		typ = ast.TFloat
		v = svar.FloatValue
	case VarDuration:
		typ = ast.TDuration
		v = svar.DurationValue
	case VarStar:
		typ = ast.TStar
		v = &ast.StarNode{}
	case VarLambda:
		typ = ast.TLambda
		l, err := ast.ParseLambda(svar.LambdaValue)
		if err != nil {
			return tick.Var{}, errors.Wrap(err, "invalid lambda expression")
		}
		v = l
	case VarString:
		typ = ast.TString
		v = svar.StringValue
	case VarRegex:
		typ = ast.TRegex
		r, err := regexp.Compile(svar.RegexValue)
		if err != nil {
			return tick.Var{}, errors.Wrap(err, "invalid regex pattern")
		}
		v = r
	case VarList:
		typ = ast.TList
		values := make([]tick.Var, len(svar.ListValue))
		var err error
		for i := range svar.ListValue {
			values[i], err = ts.convertToTickVarFromService(svar.ListValue[i])
			if err != nil {
				return tick.Var{}, err
			}
		}
		v = values
	default:
		return tick.Var{}, fmt.Errorf("invalid var: %v", svar)
	}
	return tick.Var{
		Value:       v,
		Type:        typ,
		Description: svar.Description,
	}, nil
}

func (ts *Service) convertToTickVarsFromService(svars map[string]Var) (map[string]tick.Var, error) {
	vars := make(map[string]tick.Var, len(svars))
	for name, value := range svars {
		v, err := ts.convertToTickVarFromService(value)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid var %s", name)
		}
		vars[name] = v
	}
	return vars, nil
}

func (ts *Service) handleDeleteTask(w http.ResponseWriter, r *http.Request) {
	id, err := ts.taskIDFromPath(r.URL.Path)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusBadRequest)
		return
	}

	err = ts.deleteTask(id)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (ts *Service) deleteTask(id string) error {
	// Delete associated snapshot
	ts.snapshots.Delete(id)

	// Delete task object
	task, err := ts.tasks.Get(id)
	if err != nil {
		if err == ErrNoTaskExists {
			return nil
		}
		return err
	}
	if task.TemplateID != "" {
		if err := ts.templates.DisassociateTask(task.TemplateID, task.ID); err != nil {
			ts.diag.Error("failed to disassociate task from template", err,
				keyvalue.KV("template", task.TemplateID), keyvalue.KV("task", task.ID))
		}
	}
	vars.NumTasksVar.Add(-1)
	if task.Status == Enabled {
		vars.NumEnabledTasksVar.Add(-1)
		ts.TaskMasterLookup.Main().DeleteTask(id)
	}
	return ts.tasks.Delete(id)
}

func (ts *Service) convertTemplate(t Template, scriptFormat string) (client.Template, error) {
	script := t.TICKscript
	if scriptFormat == "formatted" {
		// Format TICKscript
		formatted, err := tick.Format(script)
		if err == nil {
			// Only format if it succeeded.
			// Otherwise a change in syntax may prevent task retrieval.
			script = formatted
		}
	}

	errMsg := t.Error
	task, err := ts.templateTask(t)
	if err != nil {
		errMsg = err.Error()
	}

	var typ client.TaskType
	switch t.Type {
	case StreamTask:
		typ = client.StreamTask
	case BatchTask:
		typ = client.BatchTask
	default:
		return client.Template{}, fmt.Errorf("invalid task type %v", t.Type)
	}

	vars, err := ts.convertToClientVarsFromTick(task.Vars())
	if err != nil {
		return client.Template{}, err
	}

	return client.Template{
		Link:       ts.templateLink(t.ID),
		ID:         t.ID,
		Type:       typ,
		TICKscript: script,
		Dot:        string(task.Dot()),
		Error:      errMsg,
		Created:    t.Created,
		Modified:   t.Modified,
		Vars:       vars,
	}, nil
}

func (ts *Service) handleTemplate(w http.ResponseWriter, r *http.Request) {
	id, err := ts.templateIDFromPath(r.URL.Path)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusBadRequest)
		return
	}

	raw, err := ts.templates.Get(id)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusNotFound)
		return
	}

	scriptFormat := r.URL.Query().Get("script-format")
	switch scriptFormat {
	case "":
		scriptFormat = "formatted"
	case "formatted", "raw":
	default:
		httpd.HttpError(w, fmt.Sprintf("invalid script-format parameter %q", scriptFormat), true, http.StatusBadRequest)
		return
	}

	t, err := ts.convertTemplate(raw, scriptFormat)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(httpd.MarshalJSON(t, true))
}

var allTemplateFields = []string{
	"link",
	"id",
	"type",
	"script",
	"dot",
	"error",
	"created",
	"modified",
}

const templatesBasePathAnchored = httpd.BasePath + templatesPathAnchored

func (ts *Service) templateIDFromPath(path string) (string, error) {
	if len(path) <= len(templatesBasePathAnchored) {
		return "", errors.New("must specify template id on path")
	}
	id := path[len(templatesBasePathAnchored):]
	return id, nil
}

func (ts *Service) templateLink(id string) client.Link {
	return client.Link{Relation: client.Self, Href: path.Join(httpd.BasePath, templatesPath, id)}
}

func (ts *Service) handleListTemplates(w http.ResponseWriter, r *http.Request) {

	pattern := r.URL.Query().Get("pattern")
	fields := r.URL.Query()["fields"]
	if len(fields) == 0 {
		fields = allTemplateFields
	} else {
		// Always return ID field
		fields = append(fields, "id", "link")
	}

	scriptFormat := r.URL.Query().Get("script-format")
	switch scriptFormat {
	case "":
		scriptFormat = "formatted"
	case "formatted":
	case "raw":
	default:
		httpd.HttpError(w, fmt.Sprintf("invalid script-format parameter %q", scriptFormat), true, http.StatusBadRequest)
		return
	}

	var err error
	offset := int64(0)
	offsetStr := r.URL.Query().Get("offset")
	if offsetStr != "" {
		offset, err = strconv.ParseInt(offsetStr, 10, 64)
		if err != nil {
			httpd.HttpError(w, fmt.Sprintf("invalid offset parameter %q must be an integer: %s", offsetStr, err), true, http.StatusBadRequest)
			return
		}
	}

	limit := int64(100)
	limitStr := r.URL.Query().Get("limit")
	if limitStr != "" {
		limit, err = strconv.ParseInt(limitStr, 10, 64)
		if err != nil {
			httpd.HttpError(w, fmt.Sprintf("invalid limit parameter %q must be an integer: %s", limitStr, err), true, http.StatusBadRequest)
			return
		}
	}

	rawTemplates, err := ts.templates.List(pattern, int(offset), int(limit))
	if err != nil {
		httpd.HttpError(w, fmt.Sprintf("failed to list templates with pattern %q: %s", pattern, err), true, http.StatusBadRequest)
		return
	}
	templates := make([]map[string]interface{}, len(rawTemplates))

	for i, template := range rawTemplates {
		templates[i] = make(map[string]interface{}, len(fields))
		task, err := ts.templateTask(template)
		if err != nil {
			continue
		}
		for _, field := range fields {
			var value interface{}
			switch field {
			case "id":
				value = template.ID
			case "link":
				value = ts.templateLink(template.ID)
			case "type":
				switch template.Type {
				case StreamTask:
					value = client.StreamTask
				case BatchTask:
					value = client.BatchTask
				}
			case "script":
				value = template.TICKscript
				if scriptFormat == "formatted" {
					formatted, err := tick.Format(template.TICKscript)
					if err == nil {
						// Only format if it succeeded.
						// Otherwise a change in syntax may prevent template retrieval.
						value = formatted
					}
				}
			case "dot":
				value = string(task.Dot())
			case "vars":
				vars, err := ts.convertToClientVarsFromTick(task.Vars())
				if err != nil {
					ts.diag.Error("failed to get vars for template", err, keyvalue.KV("template", template.ID))
					break
				}
				value = vars
			case "error":
				value = template.Error
			case "created":
				value = template.Created
			case "modified":
				value = template.Modified
			default:
				httpd.HttpError(w, fmt.Sprintf("unsupported field %q", field), true, http.StatusBadRequest)
				return
			}
			templates[i][field] = value
		}
	}

	type response struct {
		Templates []map[string]interface{} `json:"templates"`
	}

	w.Write(httpd.MarshalJSON(response{templates}, true))
}

var validTemplateID = regexp.MustCompile(`^[-\._\p{L}0-9]+$`)

func (ts *Service) handleCreateTemplate(w http.ResponseWriter, r *http.Request) {
	template := client.CreateTemplateOptions{}
	dec := json.NewDecoder(r.Body)
	err := dec.Decode(&template)
	if err != nil {
		httpd.HttpError(w, "invalid JSON", true, http.StatusBadRequest)
		return
	}
	if template.ID == "" {
		template.ID = uuid.New().String()
	}
	if !validTemplateID.MatchString(template.ID) {
		httpd.HttpError(w, fmt.Sprintf("template ID must contain only letters, numbers, '-', '.' and '_'. %q", template.ID), true, http.StatusBadRequest)
		return
	}

	newTemplate := Template{
		ID: template.ID,
	}

	// Check for existing template
	_, err = ts.templates.Get(template.ID)
	if err == nil {
		httpd.HttpError(w, fmt.Sprintf("template %s already exists", template.ID), true, http.StatusBadRequest)
		return
	}

	pn, err := newProgramNodeFromTickscript(template.TICKscript)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusBadRequest)
		return
	}

	// set task type from tickscript
	switch tt := taskTypeFromProgram(pn); tt {
	case client.StreamTask:
		newTemplate.Type = StreamTask
	case client.BatchTask:
		newTemplate.Type = BatchTask
	default:
		httpd.HttpError(w, fmt.Sprintf("invalid task type: %v", tt), true, http.StatusBadRequest)
		return
	}

	// Set tick script
	newTemplate.TICKscript = template.TICKscript
	if newTemplate.TICKscript == "" {
		httpd.HttpError(w, fmt.Sprintf("must provide TICKscript"), true, http.StatusBadRequest)
		return
	}

	// Validate template
	_, err = ts.templateTask(newTemplate)
	if err != nil {
		httpd.HttpError(w, "invalid TICKscript: "+err.Error(), true, http.StatusBadRequest)
		return
	}

	now := time.Now()
	newTemplate.Created = now
	newTemplate.Modified = now

	// Save template
	err = ts.templates.Create(newTemplate)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusInternalServerError)
		return
	}

	// Return template definition
	t, err := ts.convertTemplate(newTemplate, "formatted")
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(httpd.MarshalJSON(t, true))
}

func (ts *Service) handleUpdateTemplate(w http.ResponseWriter, r *http.Request) {
	id, err := ts.templateIDFromPath(r.URL.Path)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusBadRequest)
		return
	}
	template := client.UpdateTemplateOptions{}
	dec := json.NewDecoder(r.Body)
	err = dec.Decode(&template)
	if err != nil {
		httpd.HttpError(w, "invalid JSON", true, http.StatusBadRequest)
		return
	}

	// Check for existing template
	original, err := ts.templates.Get(id)
	if err != nil {
		httpd.HttpError(w, "template does not exist, cannot update", true, http.StatusNotFound)
		return
	}
	updated := original

	// Set ID
	if template.ID != "" {
		updated.ID = template.ID
	}

	// Set template type
	switch template.Type {
	case client.StreamTask:
		updated.Type = StreamTask
	case client.BatchTask:
		updated.Type = BatchTask
	}

	// Set tick script
	if template.TICKscript != "" {
		updated.TICKscript = template.TICKscript
	}

	// Validate template
	_, err = ts.templateTask(updated)
	if err != nil {
		httpd.HttpError(w, "invalid TICKscript: "+err.Error(), true, http.StatusBadRequest)
		return
	}

	// Get associated tasks
	taskIds, err := ts.templates.ListAssociatedTasks(original.ID)
	if err != nil {
		httpd.HttpError(w, fmt.Sprintf("error getting associated tasks for template %s: %s", original.ID, err.Error()), true, http.StatusInternalServerError)
		return
	}

	// Save updated template
	now := time.Now()
	updated.Modified = now

	if original.ID != updated.ID {
		if err := ts.templates.Create(updated); err != nil {
			httpd.HttpError(w, fmt.Sprintf("failed to create new template for ID change: %s", err.Error()), true, http.StatusInternalServerError)
			return
		}
		if err := ts.templates.Delete(original.ID); err != nil {
			ts.diag.Error("failed to delete old template during ID change", err,
				keyvalue.KV("oldID", original.ID), keyvalue.KV("newID", updated.ID))
		}
	} else {
		if err := ts.templates.Replace(updated); err != nil {
			httpd.HttpError(w, fmt.Sprintf("failed to replace template definition: %s", err.Error()), true, http.StatusInternalServerError)
			return
		}
	}

	// Update all associated tasks
	err = ts.updateAllAssociatedTasks(original, updated, taskIds)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusInternalServerError)
		return
	}

	// Return template definition
	t, err := ts.convertTemplate(updated, "formatted")
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(httpd.MarshalJSON(t, true))
}

// Update all associated tasks. Return the first error if any.
// Rollsback all updated tasks if an error occurs.
func (ts *Service) updateAllAssociatedTasks(old, new Template, taskIds []string) error {
	var i int
	oldPn, err := newProgramNodeFromTickscript(old.TICKscript)
	if err != nil {
		return fmt.Errorf("failed to parse old tickscript: %v", err)
	}

	newPn, err := newProgramNodeFromTickscript(new.TICKscript)
	if err != nil {
		return fmt.Errorf("failed to parse new tickscript: %v", err)
	}

	// Setup rollback function
	defer func() {
		if i == len(taskIds) {
			// All tasks updated no need to rollback
			return
		}
		//Rollback in case of an error
		for j := 0; j <= i; j++ {
			taskId := taskIds[j]
			task, err := ts.tasks.Get(taskId)
			if err != nil {
				if err != ErrNoTaskExists {
					ts.diag.Error("error rolling back associated task", err, keyvalue.KV("task", taskId))
				}
				continue
			}
			task.TemplateID = old.ID
			task.TICKscript = old.TICKscript
			task.Type = old.Type
			if len(dbrpsFromProgram(oldPn)) > 0 {
				task.DBRPs = []DBRP{}
				for _, dbrp := range dbrpsFromProgram(oldPn) {
					task.DBRPs = append(task.DBRPs, DBRP{
						Database:        dbrp.Database,
						RetentionPolicy: dbrp.RetentionPolicy,
					})
				}
			}
			if err := ts.tasks.Replace(task); err != nil {
				ts.diag.Error("error rolling back associated task", err, keyvalue.KV("task", taskId))
			}
			if task.Status == Enabled {
				ts.stopTask(taskId)
				err := ts.startTask(task)
				if err != nil {
					ts.diag.Error("error rolling back associated task", err, keyvalue.KV("task", taskId))
				}
			}
		}
	}()

	for ; i < len(taskIds); i++ {
		taskId := taskIds[i]
		task, err := ts.tasks.Get(taskId)
		if err == ErrNoTaskExists {
			ts.templates.DisassociateTask(old.ID, taskId)
			continue
		}
		if err != nil {
			return fmt.Errorf("error retrieving associated task %s: %s", taskId, err)
		}
		if old.ID != new.ID {
			// Update association
			if err := ts.templates.AssociateTask(new.ID, taskId); err != nil {
				return fmt.Errorf("error updating task association %s: %s", taskId, err)
			}
		}

		task.TemplateID = new.ID
		task.TICKscript = new.TICKscript
		task.Type = new.Type

		if len(dbrpsFromProgram(oldPn)) > 0 || len(dbrpsFromProgram(newPn)) > 0 {

			task.DBRPs = []DBRP{}
			for _, dbrp := range dbrpsFromProgram(newPn) {
				task.DBRPs = append(task.DBRPs, DBRP{
					Database:        dbrp.Database,
					RetentionPolicy: dbrp.RetentionPolicy,
				})

			}
		}

		if err := ts.tasks.Replace(task); err != nil {
			return fmt.Errorf("error updating associated task %s: %s", taskId, err)
		}
		if task.Status == Enabled {
			ts.stopTask(taskId)
			err := ts.startTask(task)
			if err != nil {
				return fmt.Errorf("error reloading associated task %s: %s", taskId, err)
			}
		}
	}

	return nil
}

func (ts *Service) handleDeleteTemplate(w http.ResponseWriter, r *http.Request) {
	id, err := ts.templateIDFromPath(r.URL.Path)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusBadRequest)
		return
	}
	err = ts.templates.Delete(id)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (ts *Service) newKapacitorTask(task Task) (*kapacitor.Task, error) {
	dbrps := make([]kapacitor.DBRP, len(task.DBRPs))
	for i, dbrp := range task.DBRPs {
		dbrps[i] = kapacitor.DBRP{
			Database:        dbrp.Database,
			RetentionPolicy: dbrp.RetentionPolicy,
		}
	}
	var tt kapacitor.TaskType
	switch task.Type {
	case StreamTask:
		tt = kapacitor.StreamTask
	case BatchTask:
		tt = kapacitor.BatchTask
	}
	vars, err := ts.convertToTickVarsFromService(task.Vars)
	if err != nil {
		return nil, err
	}
	return ts.TaskMasterLookup.Main().NewTask(task.ID,
		task.TICKscript,
		tt,
		dbrps,
		ts.snapshotInterval,
		vars,
	)
}

func (ts *Service) templateTask(template Template) (*kapacitor.Template, error) {
	var tt kapacitor.TaskType
	switch template.Type {
	case StreamTask:
		tt = kapacitor.StreamTask
	case BatchTask:
		tt = kapacitor.BatchTask
	}
	t, err := ts.TaskMasterLookup.Main().NewTemplate(template.ID,
		template.TICKscript,
		tt,
	)
	if err != nil {
		return nil, err
	}
	return t, nil
}

func (ts *Service) startTask(task Task) error {
	t, err := ts.newKapacitorTask(task)
	if err != nil {
		return err
	}
	// Starting task, remove last error
	ts.saveLastError(t.ID, "")

	tm := ts.TaskMasterLookup.Main()
	// Start the task
	et, err := tm.StartTask(t)
	if err != nil {
		ts.saveLastError(t.ID, err.Error())
		return err
	}

	// Start batching
	if t.Type == kapacitor.BatchTask {
		err := et.StartBatching()
		if err != nil {
			ts.saveLastError(t.ID, err.Error())
			tm.StopTask(t.ID)
			return err
		}
	}

	go func() {
		// Wait for task to finish
		err := et.Wait()
		ts.diag.FinishedTask(et.Task.ID)

		if err != nil {
			// Stop task
			tm.StopTask(t.ID)

			ts.diag.Error("task finished with error", err, keyvalue.KV("task", et.Task.ID))
			// Save last error from task.
			err = ts.saveLastError(t.ID, err.Error())
			if err != nil {
				ts.diag.Error("failed to save last error for task", err, keyvalue.KV("task", et.Task.ID))
			}
		}
	}()
	return nil
}

func (ts *Service) stopTask(id string) {
	ts.TaskMasterLookup.Main().StopTask(id)
}

// Save last error from task.
func (ts *Service) saveLastError(id string, errStr string) error {
	task, err := ts.tasks.Get(id)
	if err != nil {
		return err
	}
	task.Error = errStr
	return ts.tasks.Replace(task)
}
