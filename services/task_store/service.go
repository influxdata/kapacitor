package task_store

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"time"

	"github.com/boltdb/bolt"
	"github.com/influxdata/kapacitor"
	"github.com/influxdata/kapacitor/client/v1"
	"github.com/influxdata/kapacitor/services/httpd"
	"github.com/influxdata/kapacitor/services/storage"
	"github.com/influxdata/kapacitor/tick"
	"github.com/pkg/errors"
	"github.com/twinj/uuid"
)

const (
	tasksPath         = "/tasks"
	tasksPathAnchored = "/tasks/"
)

type Service struct {
	oldDBDir         string
	tasks            TaskDAO
	snapshots        SnapshotDAO
	routes           []httpd.Route
	snapshotInterval time.Duration
	StorageService   interface {
		Store(namespace string) storage.Interface
	}
	HTTPDService interface {
		AddRoutes([]httpd.Route) error
		DelRoutes([]httpd.Route)
	}
	TaskMaster interface {
		NewTask(
			name,
			script string,
			tt kapacitor.TaskType,
			dbrps []kapacitor.DBRP,
			snapshotInterval time.Duration,
		) (*kapacitor.Task, error)
		StartTask(t *kapacitor.Task) (*kapacitor.ExecutingTask, error)
		StopTask(name string) error
		IsExecuting(name string) bool
		ExecutionStats(name string) (kapacitor.ExecutionStats, error)
		ExecutingDot(name string, labels bool) string
	}

	logger *log.Logger
}

type taskStore struct {
	Name       string
	Type       kapacitor.TaskType
	TICKScript string
}

func NewService(conf Config, l *log.Logger) *Service {
	return &Service{
		snapshotInterval: time.Duration(conf.SnapshotInterval),
		logger:           l,
		oldDBDir:         conf.Dir,
	}
}

// The storage namespace for all task data.
const taskNamespace = "task_store"

func (ts *Service) Open() error {
	// Create DAO
	store := ts.StorageService.Store(taskNamespace)
	ts.tasks = newTaskKV(store)
	ts.snapshots = newSnapshotKV(store)

	// Perform migration to new storage service.
	err := ts.migrate()
	if err != nil {
		return err
	}

	// Define API routes
	ts.routes = []httpd.Route{
		{
			Name:        "task",
			Method:      "GET",
			Pattern:     tasksPathAnchored,
			HandlerFunc: ts.handleTask,
		},
		{
			Name:        "deleteTask",
			Method:      "DELETE",
			Pattern:     tasksPathAnchored,
			HandlerFunc: ts.handleDeleteTask,
		},
		{
			// Satisfy CORS checks.
			Name:        "/tasks/-cors",
			Method:      "OPTIONS",
			Pattern:     tasksPathAnchored,
			HandlerFunc: httpd.ServeOptions,
		},
		{
			Name:        "updateTask",
			Method:      "PATCH",
			Pattern:     tasksPathAnchored,
			HandlerFunc: ts.handleUpdateTask,
		},
		{
			Name:        "listTasks",
			Method:      "GET",
			Pattern:     tasksPath,
			HandlerFunc: ts.handleListTasks,
		},
		{
			Name:        "createTask",
			Method:      "POST",
			Pattern:     tasksPath,
			HandlerFunc: ts.handleCreateTask,
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
	kapacitor.NumEnabledTasksVar.Set(0)
	for {
		tasks, err := ts.tasks.List("*", offset, limit)
		if err != nil {
			return err
		}
		for _, task := range tasks {
			numTasks++
			if task.Status == Enabled {
				ts.logger.Println("D! starting enabled task on startup", task.ID)
				err = ts.startTask(task)
				if err != nil {
					ts.logger.Printf("E! error starting enabled task %s, err: %s\n", task.ID, err)
				} else {
					ts.logger.Println("D! started task during startup", task.ID)
					numEnabledTasks++
				}
			}
		}
		if len(tasks) != limit {
			break
		}
		offset += limit
	}

	// Set expvars
	kapacitor.NumTasksVar.Set(numTasks)

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
		ts.logger.Println("W! could not open old boltd for task_store. Not performing migration. Remove the `task_store.dir` configuration to disable migration.")
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
				ts.logger.Println("E! corrupt data in old task_store boltdb tasks:", err)
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
					ts.logger.Printf("D! task %s has already been migrated skipping", task.Name)
				}
			} else {
				ts.logger.Printf("D! task %s was migrated to new storage service", task.Name)
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
				ts.logger.Println("E! corrupt data in old task_store boltdb snapshots:", err)
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
					ts.logger.Printf("D! snapshot %s was migrated to new storage service", id)
				} else {
					ts.logger.Printf("D! snapshot %s skipped, already migrated to new storage service", id)
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
		ts.logger.Println("E! error checking for snapshot", err)
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
		// Format TICKscript
		formatted, err := tick.Format(raw.TICKscript)
		if err == nil {
			// Only format if it succeeded.
			// Otherwise a change in syntax may prevent task retrieval.
			raw.TICKscript = formatted
		}
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

	executing := ts.TaskMaster.IsExecuting(id)
	errMsg := raw.Error
	dot := ""
	stats := client.ExecutionStats{}
	task, err := ts.newKapacitorTask(raw)
	if err == nil {
		if executing {
			dot = ts.TaskMaster.ExecutingDot(id, dotView == "labels")
			s, err := ts.TaskMaster.ExecutionStats(id)
			if err != nil {
				ts.logger.Printf("E! failed to retrieve stats for task %s: %v", id, err)
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
	switch raw.Status {
	case Disabled:
		status = client.Disabled
	case Enabled:
		status = client.Enabled
	default:
		httpd.HttpError(w, fmt.Sprintf("invalid task status recorded in db %v", raw.Status), true, http.StatusInternalServerError)
		return
	}

	var typ client.TaskType
	switch raw.Type {
	case StreamTask:
		typ = client.StreamTask
	case BatchTask:
		typ = client.BatchTask
	default:
		httpd.HttpError(w, fmt.Sprintf("invalid task type recorded in db %v", raw.Type), true, http.StatusInternalServerError)
		return
	}

	dbrps := make([]client.DBRP, len(raw.DBRPs))
	for i, dbrp := range raw.DBRPs {
		dbrps[i] = client.DBRP{
			Database:        dbrp.Database,
			RetentionPolicy: dbrp.RetentionPolicy,
		}
	}

	info := client.Task{
		ID:             id,
		Type:           typ,
		DBRPs:          dbrps,
		TICKscript:     raw.TICKscript,
		Dot:            dot,
		Status:         status,
		Executing:      executing,
		Error:          errMsg,
		ExecutionStats: stats,
		Created:        raw.Created,
		Modified:       raw.Modified,
		LastEnabled:    raw.LastEnabled,
	}

	w.Write(httpd.MarshalJSON(info, true))
}

var allFields = []string{
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
		fields = allFields
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
		}
	}

	limit := int64(100)
	limitStr := r.URL.Query().Get("limit")
	if limitStr != "" {
		limit, err = strconv.ParseInt(limitStr, 10, 64)
		if err != nil {
			httpd.HttpError(w, fmt.Sprintf("invalid limit parameter %q must be an integer: %s", limitStr, err), true, http.StatusBadRequest)
		}
	}

	rawTasks, err := ts.tasks.List(pattern, int(offset), int(limit))
	tasks := make([]map[string]interface{}, len(rawTasks))

	for i, task := range rawTasks {
		tasks[i] = make(map[string]interface{}, len(fields))
		executing := ts.TaskMaster.IsExecuting(task.ID)
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
					value = ts.TaskMaster.ExecutingDot(task.ID, dotView == "labels")
				} else {
					kt, err := ts.newKapacitorTask(task)
					if err != nil {
						break
					}
					value = string(kt.Dot())
				}
			case "stats":
				if executing {
					s, err := ts.TaskMaster.ExecutionStats(task.ID)
					if err != nil {
						ts.logger.Printf("E! failed to retrieve stats for task %s: %v", task.ID, err)
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
		task.ID = uuid.NewV4().String()
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

	// Set task type
	switch task.Type {
	case client.StreamTask:
		newTask.Type = StreamTask
	case client.BatchTask:
		newTask.Type = BatchTask
	default:
		httpd.HttpError(w, fmt.Sprintf("unknown type %q", task.Type), true, http.StatusBadRequest)
		return
	}

	// Set tick script
	newTask.TICKscript = task.TICKscript
	if newTask.TICKscript == "" {
		httpd.HttpError(w, fmt.Sprintf("must provide TICKscript"), true, http.StatusBadRequest)
		return
	}

	// Set dbrps
	newTask.DBRPs = make([]DBRP, len(task.DBRPs))
	for i, dbrp := range task.DBRPs {
		newTask.DBRPs[i] = DBRP{
			Database:        dbrp.Database,
			RetentionPolicy: dbrp.RetentionPolicy,
		}
	}
	if len(newTask.DBRPs) == 0 {
		httpd.HttpError(w, fmt.Sprintf("must provide at least one database and retention policy."), true, http.StatusBadRequest)
		return
	}

	// Set status
	switch task.Status {
	case client.Enabled:
		newTask.Status = Enabled
	case client.Disabled:
		newTask.Status = Disabled
	default:
		task.Status = client.Disabled
		newTask.Status = Disabled
	}

	// Validate task
	ktask, err := ts.newKapacitorTask(newTask)
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

	err = ts.tasks.Create(newTask)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusInternalServerError)
		return
	}
	if newTask.Status == Enabled {
		err = ts.startTask(newTask)
		if err != nil {
			httpd.HttpError(w, err.Error(), true, http.StatusInternalServerError)
			return
		}
	}
	w.WriteHeader(http.StatusOK)

	executing := ts.TaskMaster.IsExecuting(newTask.ID)
	dot := ""
	stats := client.ExecutionStats{}
	if executing {
		dot = ts.TaskMaster.ExecutingDot(newTask.ID, false)
		s, err := ts.TaskMaster.ExecutionStats(newTask.ID)
		if err != nil {
			ts.logger.Printf("E! failed to retrieve stats for task %s: %v", newTask.ID, err)
		} else {
			stats.TaskStats = s.TaskStats
			stats.NodeStats = s.NodeStats
		}
	} else {
		dot = string(ktask.Dot())
	}

	t := client.Task{
		Link:           ts.taskLink(newTask.ID),
		ID:             newTask.ID,
		Type:           task.Type,
		DBRPs:          task.DBRPs,
		TICKscript:     task.TICKscript,
		Status:         task.Status,
		Dot:            dot,
		Executing:      executing,
		ExecutionStats: stats,
		Created:        newTask.Created,
		Modified:       newTask.Modified,
		LastEnabled:    newTask.LastEnabled,
	}
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
	existing, err := ts.tasks.Get(id)
	if err != nil {
		httpd.HttpError(w, "task does not exist, cannot update", true, http.StatusNotFound)
		return
	}

	// Set task type
	switch task.Type {
	case client.StreamTask:
		existing.Type = StreamTask
	case client.BatchTask:
		existing.Type = BatchTask
	}

	// Set tick script
	if task.TICKscript != "" {
		existing.TICKscript = task.TICKscript
	}

	// Set dbrps
	if len(task.DBRPs) > 0 {
		existing.DBRPs = make([]DBRP, len(task.DBRPs))
		for i, dbrp := range task.DBRPs {
			existing.DBRPs[i] = DBRP{
				Database:        dbrp.Database,
				RetentionPolicy: dbrp.RetentionPolicy,
			}
		}
	}

	// Set status
	previousStatus := existing.Status
	switch task.Status {
	case client.Enabled:
		existing.Status = Enabled
	case client.Disabled:
		existing.Status = Disabled
	}
	statusChanged := previousStatus != existing.Status

	// Validate task
	_, err = ts.newKapacitorTask(existing)
	if err != nil {
		httpd.HttpError(w, "invalid TICKscript: "+err.Error(), true, http.StatusBadRequest)
		return
	}

	now := time.Now()
	existing.Modified = now
	if statusChanged && existing.Status == Enabled {
		existing.LastEnabled = now
	}
	err = ts.tasks.Replace(existing)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusInternalServerError)
		return
	}

	if statusChanged {
		// Enable/Disable task
		switch existing.Status {
		case Enabled:
			err = ts.startTask(existing)
			if err != nil {
				httpd.HttpError(w, err.Error(), true, http.StatusInternalServerError)
				return
			}
		case Disabled:
			ts.stopTask(existing.ID)
		}
	}

	w.WriteHeader(http.StatusNoContent)
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
	task, err := ts.tasks.Get(id)
	if err != nil {
		if err == ErrNoTaskExists {
			return nil
		}
		return err
	}
	if task.Status == Enabled {
		ts.stopTask(id)
	}
	return ts.tasks.Delete(id)
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
	return ts.TaskMaster.NewTask(task.ID,
		task.TICKscript,
		tt,
		dbrps,
		ts.snapshotInterval,
	)
}

func (ts *Service) startTask(task Task) error {
	t, err := ts.newKapacitorTask(task)
	if err != nil {
		return err
	}
	// Starting task, remove last error
	ts.saveLastError(t.ID, "")

	// Start the task
	et, err := ts.TaskMaster.StartTask(t)
	if err != nil {
		ts.saveLastError(t.ID, err.Error())
		return err
	}

	// Start batching
	if t.Type == kapacitor.BatchTask {
		err := et.StartBatching()
		if err != nil {
			ts.saveLastError(t.ID, err.Error())
			ts.TaskMaster.StopTask(t.ID)
			return err
		}
	}

	kapacitor.NumEnabledTasksVar.Add(1)

	go func() {
		// Wait for task to finish
		err := et.Wait()
		// Stop task
		ts.stopTask(t.ID)

		if err != nil {
			ts.logger.Printf("E! task %s finished with error: %s", et.Task.ID, err)
			// Save last error from task.
			err = ts.saveLastError(t.ID, err.Error())
			if err != nil {
				ts.logger.Println("E! failed to save last error for task", et.Task.ID)
			}
		}
	}()
	return nil
}

func (ts *Service) stopTask(id string) {
	kapacitor.NumEnabledTasksVar.Add(-1)
	ts.TaskMaster.StopTask(id)
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
