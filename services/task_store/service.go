package task_store

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/boltdb/bolt"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/kapacitor"
	"github.com/influxdata/kapacitor/services/httpd"
	"github.com/influxdata/kapacitor/tick"
)

const taskDB = "task.db"

var (
	tasksBucket    = []byte("tasks")
	enabledBucket  = []byte("enabled")
	snapshotBucket = []byte("snapshots")
)

type Service struct {
	dbpath           string
	db               *bolt.DB
	routes           []httpd.Route
	snapshotInterval time.Duration
	HTTPDService     interface {
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
		dbpath:           path.Join(conf.Dir, taskDB),
		snapshotInterval: time.Duration(conf.SnapshotInterval),
		logger:           l,
	}
}

func (ts *Service) Open() error {
	err := os.MkdirAll(path.Dir(ts.dbpath), 0755)
	if err != nil {
		return err
	}

	// Open db
	db, err := bolt.Open(ts.dbpath, 0600, nil)
	if err != nil {
		return err
	}
	ts.db = db

	// Define API routes
	ts.routes = []httpd.Route{
		{
			Name:        "task-show",
			Method:      "GET",
			Pattern:     "/task",
			HandlerFunc: ts.handleTask,
		},
		{
			Name:        "task-list",
			Method:      "GET",
			Pattern:     "/tasks",
			HandlerFunc: ts.handleTasks,
		},
		{
			Name:        "task-save",
			Method:      "POST",
			Pattern:     "/task",
			HandlerFunc: ts.handleSave,
		},
		{
			Name:        "task-delete",
			Method:      "DELETE",
			Pattern:     "/task",
			HandlerFunc: ts.handleDelete,
		},
		{
			// Satisfy CORS checks.
			Name:        "task-delete",
			Method:      "OPTIONS",
			Pattern:     "/task",
			HandlerFunc: httpd.ServeOptions,
		},
		{
			Name:        "task-enable",
			Method:      "POST",
			Pattern:     "/enable",
			HandlerFunc: ts.handleEnable,
		},
		{
			Name:        "task-disable",
			Method:      "POST",
			Pattern:     "/disable",
			HandlerFunc: ts.handleDisable,
		},
	}
	err = ts.HTTPDService.AddRoutes(ts.routes)
	if err != nil {
		return err
	}

	numTasks := int64(0)
	numEnabledTasks := int64(0)

	// Count all tasks
	err = ts.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(tasksBucket))
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			numTasks++
			return nil
		})
	})
	if err != nil {
		return err
	}

	// Get enabled tasks
	enabledTasks := make([]string, 0)
	err = ts.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(enabledBucket))
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			enabledTasks = append(enabledTasks, string(k))
			return nil
		})
	})
	if err != nil {
		return err
	}

	// Start each enabled task
	for _, name := range enabledTasks {
		ts.logger.Println("D! starting enabled task on startup", name)
		t, err := ts.Load(name)
		if err != nil {
			ts.logger.Printf("E! error loading enabled task %s, err: %s\n", name, err)
			return nil
		}
		err = ts.StartTask(t)
		if err != nil {
			ts.logger.Printf("E! error starting enabled task %s, err: %s\n", name, err)
		} else {
			ts.logger.Println("D! started task during startup", name)
			numEnabledTasks++
		}
	}

	// Set expvars
	kapacitor.NumTasksVar.Set(numTasks)
	kapacitor.NumEnabledTasksVar.Set(numEnabledTasks)
	return nil
}

func (ts *Service) Close() error {
	ts.HTTPDService.DelRoutes(ts.routes)
	if ts.db != nil {
		return ts.db.Close()
	}
	return nil
}

func (ts *Service) SaveSnapshot(name string, snapshot *kapacitor.TaskSnapshot) error {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(snapshot)
	if err != nil {
		return fmt.Errorf("failed to encode task snapshot %s %v", name, err)
	}

	err = ts.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(snapshotBucket)
		if err != nil {
			return err
		}

		return b.Put([]byte(name), buf.Bytes())
	})
	if err != nil {
		return err
	}
	return nil
}
func (ts *Service) HasSnapshot(name string) bool {
	err := ts.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(snapshotBucket)
		if b == nil {
			return fmt.Errorf("no snapshot found for task %s", name)
		}

		data := b.Get([]byte(name))
		if data == nil {
			return fmt.Errorf("no snapshot found for task %s", name)
		}
		return nil
	})
	return err == nil
}

func (ts *Service) LoadSnapshot(name string) (*kapacitor.TaskSnapshot, error) {
	var data []byte
	err := ts.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(snapshotBucket)
		if b == nil {
			return fmt.Errorf("no snapshot found for task %s", name)
		}

		data = b.Get([]byte(name))
		if data == nil {
			return fmt.Errorf("no snapshot found for task %s", name)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	snapshot := &kapacitor.TaskSnapshot{}
	err = dec.Decode(snapshot)
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

type TaskInfo struct {
	Name           string
	Type           kapacitor.TaskType
	DBRPs          []kapacitor.DBRP
	TICKscript     string
	Dot            string
	Enabled        bool
	Executing      bool
	Error          string
	ExecutionStats kapacitor.ExecutionStats
}

func (ts *Service) handleTask(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	if name == "" {
		httpd.HttpError(w, "must pass task name", true, http.StatusBadRequest)
		return
	}
	labels := false
	labelsStr := r.URL.Query().Get("labels")
	if labelsStr != "" {
		var err error
		labels, err = strconv.ParseBool(labelsStr)
		if err != nil {
			httpd.HttpError(w, "invalid labels value:", true, http.StatusBadRequest)
			return
		}

	}

	raw, err := ts.LoadRaw(name)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusNotFound)
		return
	}

	executing := ts.TaskMaster.IsExecuting(name)
	errMsg := raw.Error
	dot := ""
	stats := kapacitor.ExecutionStats{}
	task, err := ts.Load(name)
	if err == nil {
		if executing {
			dot = ts.TaskMaster.ExecutingDot(name, labels)
			stats, _ = ts.TaskMaster.ExecutionStats(name)
		} else {
			dot = string(task.Dot())
		}
	} else {
		errMsg = err.Error()
	}

	info := TaskInfo{
		Name:           name,
		Type:           raw.Type,
		DBRPs:          raw.DBRPs,
		TICKscript:     raw.TICKscript,
		Dot:            dot,
		Enabled:        ts.IsEnabled(name),
		Executing:      executing,
		Error:          errMsg,
		ExecutionStats: stats,
	}

	w.Write(httpd.MarshalJSON(info, true))
}

func (ts *Service) handleTasks(w http.ResponseWriter, r *http.Request) {
	tasksStr := r.URL.Query().Get("tasks")
	var tasks []string
	if tasksStr != "" {
		tasks = strings.Split(tasksStr, ",")
	}

	infos, err := ts.GetTaskSummaryInfo(tasks)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusNotFound)
		return
	}

	type response struct {
		Tasks []TaskSummaryInfo `json:"Tasks"`
	}

	w.Write(httpd.MarshalJSON(response{infos}, true))
}

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

func (ts *Service) handleSave(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	newTask := &rawTask{
		Name:             name,
		SnapshotInterval: ts.snapshotInterval,
	}

	// Check for existing task
	raw, err := ts.LoadRaw(name)
	exists := err == nil
	if exists {
		newTask = raw
	}

	// Get task type
	ttStr := r.URL.Query().Get("type")
	switch ttStr {
	case "stream":
		newTask.Type = kapacitor.StreamTask
	case "batch":
		newTask.Type = kapacitor.BatchTask
	default:
		if !exists {
			if ttStr == "" {
				httpd.HttpError(w, fmt.Sprintf("no task with name %q exists cannot infer type.", name), true, http.StatusBadRequest)
			} else {
				httpd.HttpError(w, fmt.Sprintf("unknown type %q", ttStr), true, http.StatusBadRequest)
			}
			return
		}
	}

	// Get tick script
	tick, err := ioutil.ReadAll(r.Body)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusBadRequest)
		return
	}
	if len(tick) > 0 {
		newTask.TICKscript = string(tick)
	} else if !exists {
		httpd.HttpError(w, fmt.Sprintf("must provide TICKscript via POST data."), true, http.StatusBadRequest)
		return
	}

	// Get dbrps
	dbrpsStr := r.URL.Query().Get("dbrps")
	if dbrpsStr != "" {
		dbrps := make([]kapacitor.DBRP, 0)
		err = json.Unmarshal([]byte(dbrpsStr), &dbrps)
		if err != nil {
			httpd.HttpError(w, err.Error(), true, http.StatusBadRequest)
			return
		}
		newTask.DBRPs = dbrps
	} else if !exists {
		httpd.HttpError(w, fmt.Sprintf("must provide at least one database and retention policy."), true, http.StatusBadRequest)
		return
	}

	// Get snapshot interval
	snapshotIntervalStr := r.URL.Query().Get("snapshot")
	if snapshotIntervalStr != "" {
		snapshotInterval, err := influxql.ParseDuration(snapshotIntervalStr)
		if err != nil {
			httpd.HttpError(w, err.Error(), true, http.StatusBadRequest)
			return
		}
		newTask.SnapshotInterval = snapshotInterval
	}

	err = ts.Save(newTask)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusInternalServerError)
		return
	}
}

func (ts *Service) handleDelete(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")

	err := ts.Delete(name)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusInternalServerError)
		return
	}
}

func (ts *Service) handleEnable(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	err := ts.Enable(name)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusInternalServerError)
		return
	}
}

func (ts *Service) handleDisable(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	err := ts.Disable(name)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusInternalServerError)
		return
	}
}

func (ts *Service) Save(task *rawTask) error {

	// Format TICKscript
	formatted, err := tick.Format(task.TICKscript)
	if err != nil {
		return err
	}
	task.TICKscript = formatted

	// Validate task
	_, err = ts.TaskMaster.NewTask(task.Name,
		task.TICKscript,
		task.Type,
		task.DBRPs,
		task.SnapshotInterval,
	)
	if err != nil {
		return fmt.Errorf("invalid task: %s", err)
	}

	// Write 0 snapshot interval if it is the default.
	// This way if the default changes the task will change too.
	if task.SnapshotInterval == ts.snapshotInterval {
		task.SnapshotInterval = 0
	}

	var buf bytes.Buffer

	enc := gob.NewEncoder(&buf)
	err = enc.Encode(task)
	if err != nil {
		return err
	}

	err = ts.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(tasksBucket)
		if err != nil {
			return err
		}
		exists := b.Get([]byte(task.Name)) != nil
		err = b.Put([]byte(task.Name), buf.Bytes())
		if err != nil {
			return err
		}
		if !exists {
			kapacitor.NumTasksVar.Add(1)
		}
		return nil
	})
	return err
}

func (ts *Service) deleteTask(name string) error {
	ts.TaskMaster.StopTask(name)

	return ts.db.Update(func(tx *bolt.Tx) error {
		tb := tx.Bucket(tasksBucket)
		if tb != nil {
			exists := tb.Get([]byte(name)) != nil
			if exists {
				tb.Delete([]byte(name))
				kapacitor.NumTasksVar.Add(-1)
			}
		}
		eb := tx.Bucket(enabledBucket)
		if eb != nil {
			eb.Delete([]byte(name))
		}
		return nil
	})
}

func (ts *Service) Delete(pattern string) error {
	rawTasks, err := ts.FindTasks(func(taskName string) (bool, error) {
		matched, err := filepath.Match(pattern, taskName)
		if err != nil {
			return false, err
		}

		return matched, nil
	})

	if err != nil {
		return nil
	}

	for _, rawTask := range rawTasks {
		err = ts.deleteTask(rawTask.Name)
		if err != nil {
			return err
		}
	}

	return nil
}

func (ts *Service) LoadRaw(name string) (*rawTask, error) {
	var data []byte
	err := ts.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(tasksBucket)
		if b == nil {
			return errors.New("no tasks bucket")
		}
		data = b.Get([]byte(name))
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("unknown task %s", name)
	}
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	task := &rawTask{}
	err = dec.Decode(task)
	if task.SnapshotInterval == 0 {
		task.SnapshotInterval = ts.snapshotInterval
	}
	if err != nil {
		return nil, err
	}

	return task, nil
}
func (ts *Service) Load(name string) (*kapacitor.Task, error) {
	task, err := ts.LoadRaw(name)
	if err != nil {
		return nil, err
	}

	return ts.CreateTaskFromRaw(task)
}

func (ts *Service) CreateTaskFromRaw(task *rawTask) (*kapacitor.Task, error) {
	return ts.TaskMaster.NewTask(task.Name,
		task.TICKscript,
		task.Type,
		task.DBRPs,
		task.SnapshotInterval,
	)
}

func (ts *Service) enableRawTask(rawTask *rawTask) error {
	t, err := ts.CreateTaskFromRaw(rawTask)
	if err != nil {
		return err
	}

	// Save the enabled state
	var enabled bool

	err = ts.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(enabledBucket)
		if err != nil {
			return err
		}
		enabled = b.Get([]byte(t.Name)) != nil
		err = b.Put([]byte(t.Name), []byte{})
		if err != nil {
			return err
		}
		if !enabled {
			kapacitor.NumEnabledTasksVar.Add(1)
		}
		return nil
	})
	if err != nil {
		return err
	}

	if !enabled {
		return ts.StartTask(t)
	}
	return nil
}

func (ts *Service) Enable(pattern string) error {
	// Find the matching tasks
	rawTasks, err := ts.FindTasks(func(taskName string) (bool, error) {
		matched, err := filepath.Match(pattern, taskName)
		if err != nil {
			return false, err
		}

		return matched, nil
	})

	if err != nil {
		return nil
	}

	for _, rawTask := range rawTasks {
		err = ts.enableRawTask(rawTask)
		if err != nil {
			return nil
		}
	}

	return nil
}

func (ts *Service) StartTask(t *kapacitor.Task) error {
	// Starting task, remove last error
	ts.SaveLastError(t.Name, "")

	// Start the task
	et, err := ts.TaskMaster.StartTask(t)
	if err != nil {
		ts.SaveLastError(t.Name, err.Error())
		return err
	}

	// Start batching
	if t.Type == kapacitor.BatchTask {
		err := et.StartBatching()
		if err != nil {
			ts.SaveLastError(t.Name, err.Error())
			ts.TaskMaster.StopTask(t.Name)
			return err
		}
	}

	go func() {
		// Wait for task to finish
		err := et.Wait()
		// Stop task
		ts.TaskMaster.StopTask(et.Task.Name)

		if err != nil {
			ts.logger.Printf("E! task %s finished with error: %s", et.Task.Name, err)
			// Save last error from task.
			err = ts.SaveLastError(t.Name, err.Error())
			if err != nil {
				ts.logger.Println("E! failed to save last error for task", et.Task.Name)
			}
		}
	}()
	return nil
}

// Save last error from task.
func (ts *Service) SaveLastError(name string, errStr string) error {

	raw, err := ts.LoadRaw(name)
	if err != nil {
		return err
	}
	raw.Error = errStr
	err = ts.Save(raw)
	if err != nil {
		return err
	}
	return nil
}

func (ts *Service) Disable(pattern string) error {
	// Find the matching tasks
	rawTasks, err := ts.FindTasks(func(taskName string) (bool, error) {
		matched, err := filepath.Match(pattern, taskName)
		if err != nil {
			return false, err
		}

		return matched, nil
	})

	if err != nil {
		return nil
	}

	// Delete the enabled state
	err = ts.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(enabledBucket)
		if err != nil {
			return err
		}
		for _, rawTask := range rawTasks {
			enabled := b.Get([]byte(rawTask.Name)) != nil
			if enabled {
				err = b.Delete([]byte(rawTask.Name))
				if err != nil {
					return err
				}
				kapacitor.NumEnabledTasksVar.Add(-1)
			}
		}
		return nil
	})

	if err != nil {
		return err
	}

	for _, rawTask := range rawTasks {
		err = ts.TaskMaster.StopTask(rawTask.Name)
		if err != nil {
			return err
		}
	}

	return nil
}

type TaskSummaryInfo struct {
	Name           string
	Type           kapacitor.TaskType
	DBRPs          []kapacitor.DBRP
	Enabled        bool
	Executing      bool
	ExecutionStats kapacitor.ExecutionStats
}

func (ts *Service) IsEnabled(name string) (e bool) {
	ts.db.View(func(tx *bolt.Tx) error {
		eb := tx.Bucket([]byte(enabledBucket))
		e = eb != nil && eb.Get([]byte(name)) != nil
		return nil
	})
	return
}

// Returns all taskInfo of task name that matches the predicate
func (ts *Service) FindTasks(predicate func(string) (bool, error)) ([]*rawTask, error) {
	rawTasks := make([]*rawTask, 0)

	err := ts.db.View(func(tx *bolt.Tx) error {
		tb := tx.Bucket([]byte(tasksBucket))
		if tb == nil {
			return nil
		}

		return tb.ForEach(func(k, v []byte) error {
			taskName := string(k)
			isMatched, err := predicate(taskName)
			if err != nil {
				return err
			}
			if !isMatched {
				return nil
			}

			// Grab task info
			t, err := ts.LoadRaw(taskName)
			if err != nil {
				return fmt.Errorf("found invalid task in db. name: %s, err: %s", string(k), err)
			}

			rawTasks = append(rawTasks, t)
			return nil
		})

	})
	if err != nil {
		return nil, err
	}

	return rawTasks, nil
}

func (ts *Service) GetTaskSummaryInfo(tasks []string) ([]TaskSummaryInfo, error) {
	taskInfos := make([]TaskSummaryInfo, 0)

	err := ts.db.View(func(tx *bolt.Tx) error {
		tb := tx.Bucket([]byte(tasksBucket))
		if tb == nil {
			return nil
		}
		eb := tx.Bucket([]byte(enabledBucket))
		// Grab task info
		f := func(k, v []byte) error {
			t, err := ts.LoadRaw(string(k))
			if err != nil {
				return fmt.Errorf("found invalid task in db. name: %s, err: %s", string(k), err)
			}

			enabled := eb != nil && eb.Get(k) != nil

			info := TaskSummaryInfo{
				Name:      t.Name,
				Type:      t.Type,
				DBRPs:     t.DBRPs,
				Enabled:   enabled,
				Executing: ts.TaskMaster.IsExecuting(t.Name),
			}

			if info.Executing {
				executionStats, err := ts.TaskMaster.ExecutionStats(t.Name)
				if err != nil {
					return fmt.Errorf("failed to fetch execution stats. name: %s, err: %s", t.Name, err)
				}
				info.ExecutionStats = executionStats
			}

			taskInfos = append(taskInfos, info)
			return nil
		}

		if len(tasks) == 0 {
			return tb.ForEach(f)
		} else {
			for _, tn := range tasks {
				err := f([]byte(tn), []byte{})
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return taskInfos, nil
}
