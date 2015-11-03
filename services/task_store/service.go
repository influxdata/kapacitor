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
	"strings"

	"github.com/boltdb/bolt"
	"github.com/influxdb/kapacitor"
	"github.com/influxdb/kapacitor/services/httpd"
)

const taskDB = "task.db"

var (
	tasksBucket   = []byte("tasks")
	enabledBucket = []byte("enabled")
)

type Service struct {
	dbpath       string
	db           *bolt.DB
	routes       []httpd.Route
	HTTPDService interface {
		AddRoutes([]httpd.Route) error
		DelRoutes([]httpd.Route)
	}
	TaskMaster interface {
		StartTask(t *kapacitor.Task) (*kapacitor.ExecutingTask, error)
		StopTask(name string)
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
		dbpath: path.Join(conf.Dir, taskDB),
		logger: l,
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
			"task-show",
			"GET",
			"/task",
			true,
			true,
			ts.handleTask,
		},
		{
			"task-list",
			"GET",
			"/tasks",
			true,
			true,
			ts.handleTasks,
		},
		{
			"task-save",
			"POST",
			"/task",
			true,
			true,
			ts.handleSave,
		},
		{
			"task-delete",
			"DELETE",
			"/task",
			true,
			true,
			ts.handleDelete,
		},
		{
			"task-enable",
			"POST",
			"/enable",
			true,
			true,
			ts.handleEnable,
		},
		{
			"task-disable",
			"POST",
			"/disable",
			true,
			true,
			ts.handleDisable,
		},
	}
	err = ts.HTTPDService.AddRoutes(ts.routes)
	if err != nil {
		return err
	}

	// Enable tasks
	return ts.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(enabledBucket))
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			err := ts.Enable(string(k))
			if err != nil {
				ts.logger.Printf("E! error enabling task %s, err: %s\n", string(k), err)
			}
			return nil
		})
	})
}

func (ts *Service) Close() error {
	ts.HTTPDService.DelRoutes(ts.routes)
	if ts.db != nil {
		return ts.db.Close()
	}
	return nil
}

func (ts *Service) handleTask(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("task")
	if name == "" {
		httpd.HttpError(w, "must pass task name", true, http.StatusBadRequest)
		return
	}

	raw, err := ts.LoadRaw(name)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusNotFound)
		return
	}
	task, err := ts.Load(name)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusNotFound)
		return
	}

	type response struct {
		Name       string
		Type       kapacitor.TaskType
		DBRPs      []kapacitor.DBRP
		TICKscript string
		Dot        string
		Enabled    bool
	}

	res := response{
		Name:       name,
		Type:       task.Type,
		DBRPs:      task.DBRPs,
		TICKscript: raw.TICKscript,
		Dot:        string(task.Dot()),
		Enabled:    ts.IsEnabled(name),
	}

	w.Write(httpd.MarshalJSON(res, true))
}

func (ts *Service) handleTasks(w http.ResponseWriter, r *http.Request) {
	tasksStr := r.URL.Query().Get("tasks")
	var tasks []string
	if tasksStr != "" {
		tasks = strings.Split(tasksStr, ",")
	}

	infos, err := ts.GetTaskInfo(tasks)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusNotFound)
		return
	}

	type response struct {
		Tasks []taskInfo `json:"Tasks"`
	}

	w.Write(httpd.MarshalJSON(response{infos}, true))
}

type rawTask struct {
	Name       string
	TICKscript string
	Type       kapacitor.TaskType
	DBRPs      []kapacitor.DBRP
}

func (ts *Service) handleSave(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	newTask := &rawTask{
		Name: name,
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

	// Validate task
	_, err := kapacitor.NewTask(task.Name, task.TICKscript, task.Type, task.DBRPs)
	if err != nil {
		return fmt.Errorf("invalid task: %s", err)
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
		return b.Put([]byte(task.Name), buf.Bytes())
	})
	return err
}

func (ts *Service) Delete(name string) error {

	ts.TaskMaster.StopTask(name)
	return ts.db.Update(func(tx *bolt.Tx) error {
		tb := tx.Bucket(tasksBucket)
		if tb != nil {
			tb.Delete([]byte(name))
		}
		eb := tx.Bucket(enabledBucket)
		if eb != nil {
			eb.Delete([]byte(name))
		}
		return nil
	})
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
	err = dec.Decode(&task)
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
	return kapacitor.NewTask(task.Name, task.TICKscript, task.Type, task.DBRPs)
}

func (ts *Service) Enable(name string) error {
	// Load the task
	t, err := ts.Load(name)
	if err != nil {
		return err
	}

	// Save the enabled state
	err = ts.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(enabledBucket)
		if err != nil {
			return err
		}
		return b.Put([]byte(name), []byte{})
	})

	// Start the task
	et, err := ts.TaskMaster.StartTask(t)
	if err != nil {
		return err
	}

	// Start batching
	if t.Type == kapacitor.BatchTask {
		err := et.StartBatching()
		if err != nil {
			return err
		}
	}
	return nil
}

func (ts *Service) Disable(name string) error {
	// Delete the enabled state
	err := ts.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(enabledBucket)
		if err != nil {
			return err
		}
		return b.Delete([]byte(name))
	})
	ts.TaskMaster.StopTask(name)
	return err
}

type taskInfo struct {
	Name    string
	Type    kapacitor.TaskType
	DBRPs   []kapacitor.DBRP
	Enabled bool
}

func (ts *Service) IsEnabled(name string) (e bool) {
	ts.db.View(func(tx *bolt.Tx) error {
		eb := tx.Bucket([]byte(enabledBucket))
		e = eb != nil && eb.Get([]byte(name)) != nil
		return nil
	})
	return
}

func (ts *Service) GetTaskInfo(tasks []string) ([]taskInfo, error) {
	taskInfos := make([]taskInfo, 0)

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

			info := taskInfo{
				Name:    t.Name,
				Type:    t.Type,
				DBRPs:   t.DBRPs,
				Enabled: enabled,
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
