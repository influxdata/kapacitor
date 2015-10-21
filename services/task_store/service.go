package task_store

import (
	"bytes"
	"encoding/gob"
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

func NewService(conf Config) *Service {
	return &Service{
		dbpath: path.Join(conf.Dir, taskDB),
		logger: log.New(os.Stderr, "[task] ", log.LstdFlags),
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

func (ts *Service) handleSave(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	var tt kapacitor.TaskType
	ttStr := r.URL.Query().Get("type")
	switch ttStr {
	case "stream":
		tt = kapacitor.StreamerTask
	case "batch":
		tt = kapacitor.BatcherTask
	default:
		httpd.HttpError(w, fmt.Sprintf("unknown type %q", ttStr), true, http.StatusBadRequest)
		return
	}
	tick, err := ioutil.ReadAll(r.Body)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusNoContent)
		return
	}

	err = ts.Save(name, string(tick), tt)
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

func (ts *Service) Save(name, tick string, tt kapacitor.TaskType) error {

	// Validate task
	_, err := kapacitor.NewTask(name, tick, tt)
	if err != nil {
		return fmt.Errorf("invalid task: %s", err)
	}

	var buf bytes.Buffer
	task := taskStore{
		Name:       name,
		TICKScript: tick,
		Type:       tt,
	}

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
		return b.Put([]byte(name), buf.Bytes())
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

func (ts *Service) Load(name string) (*kapacitor.Task, error) {
	var data []byte
	err := ts.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(tasksBucket)
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
	task := taskStore{}
	err = dec.Decode(&task)
	if err != nil {
		return nil, err
	}

	return kapacitor.NewTask(task.Name, task.TICKScript, task.Type)
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
	if t.Type == kapacitor.BatcherTask {
		et.StartBatching()
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
	Enabled bool
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
			t, err := ts.Load(string(k))
			if err != nil {
				return fmt.Errorf("found invalid task in db. name: %s, err: %s", string(k), err)
			}

			enabled := eb != nil && eb.Get(k) != nil

			info := taskInfo{
				Name:    t.Name,
				Type:    t.Type,
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
