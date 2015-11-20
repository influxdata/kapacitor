package kapacitor

import (
	"fmt"
	"log"
	"net"
	"sync"

	"github.com/influxdb/influxdb/client"
	"github.com/influxdb/kapacitor/models"
	"github.com/influxdb/kapacitor/pipeline"
	"github.com/influxdb/kapacitor/services/httpd"
)

type LogService interface {
	NewLogger(prefix string, flag int) *log.Logger
}

// An execution framework for  a set of tasks.
type TaskMaster struct {
	Stream       StreamCollector
	HTTPDService interface {
		AddRoutes([]httpd.Route) error
		DelRoutes([]httpd.Route)
		Addr() net.Addr
	}
	InfluxDBService interface {
		NewClient() (*client.Client, error)
	}
	SMTPService interface {
		SendMail(from string, to []string, subject string, msg string)
	}
	LogService LogService

	// Incoming stream and forks
	in    *Edge
	forks map[string]fork

	// Set of incoming batches
	batches map[string][]BatchCollector

	// Executing tasks
	tasks map[string]*ExecutingTask

	logger *log.Logger

	mu sync.RWMutex
}

// A fork of the main data stream filtered by a set of DBRPs
type fork struct {
	Edge  *Edge
	dbrps map[DBRP]bool
}

// Create a new Executor with a given clock.
func NewTaskMaster(l LogService) *TaskMaster {
	src := newEdge("TASK_MASTER", "sources", "stream", pipeline.StreamEdge, l)
	return &TaskMaster{
		Stream:     src,
		in:         src,
		forks:      make(map[string]fork),
		batches:    make(map[string][]BatchCollector),
		tasks:      make(map[string]*ExecutingTask),
		LogService: l,
		logger:     l.NewLogger("[task_master] ", log.LstdFlags),
	}
}

// Returns a new TaskMaster instance with the same services as the current one.
func (tm *TaskMaster) New() *TaskMaster {
	n := NewTaskMaster(tm.LogService)
	n.HTTPDService = tm.HTTPDService
	n.InfluxDBService = tm.InfluxDBService
	n.SMTPService = tm.SMTPService
	return n
}

func (tm *TaskMaster) Open() error {

	go tm.runForking()
	return nil
}

func (tm *TaskMaster) Close() error {
	tm.in.Close()
	for _, et := range tm.tasks {
		tm.StopTask(et.Task.Name)
	}
	return nil
}

func (tm *TaskMaster) StartTask(t *Task) (*ExecutingTask, error) {
	tm.logger.Println("D! Starting task:", t.Name)
	et, err := NewExecutingTask(tm, t)
	if err != nil {
		return nil, err
	}

	var ins []*Edge
	switch et.Task.Type {
	case StreamTask:
		ins = []*Edge{tm.NewFork(et.Task.Name, et.Task.DBRPs)}
	case BatchTask:
		count, err := et.BatchCount()
		if err != nil {
			return nil, err
		}
		ins = make([]*Edge, count)
		for i := 0; i < count; i++ {
			in := newEdge(t.Name, "batch", fmt.Sprintf("batch%d", i), pipeline.BatchEdge, tm.LogService)
			ins[i] = in
			tm.batches[t.Name] = append(tm.batches[t.Name], in)
		}

	}

	err = et.start(ins)
	if err != nil {
		return nil, err
	}

	tm.tasks[et.Task.Name] = et
	tm.logger.Println("I! Started task:", t.Name)
	tm.logger.Println("D!", string(t.Dot()))

	return et, nil
}

func (tm *TaskMaster) BatchCollectors(name string) []BatchCollector {
	return tm.batches[name]
}

func (tm *TaskMaster) StopTask(name string) {
	if et, ok := tm.tasks[name]; ok {
		delete(tm.tasks, name)
		if et.Task.Type == StreamTask {
			tm.DelFork(et.Task.Name)
		}
		et.stop()
		tm.logger.Println("I! Stopped task:", name)
	}
}

func (tm *TaskMaster) runForking() {
	for p, ok := tm.in.NextPoint(); ok; p, ok = tm.in.NextPoint() {
		tm.forkPoint(p)
	}
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	for _, fork := range tm.forks {
		fork.Edge.Close()
	}
}

func (tm *TaskMaster) forkPoint(p models.Point) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	for name, fork := range tm.forks {
		dbrp := DBRP{
			Database:        p.Database,
			RetentionPolicy: p.RetentionPolicy,
		}
		if fork.dbrps[dbrp] {
			err := fork.Edge.CollectPoint(p)
			if err != nil {
				tm.StopTask(name)
			}
		}
	}
}

func (tm *TaskMaster) NewFork(taskName string, dbrps []DBRP) *Edge {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	e := newEdge(taskName, "stream", "stream0", pipeline.StreamEdge, tm.LogService)
	tm.forks[taskName] = fork{
		Edge:  e,
		dbrps: CreateDBRPMap(dbrps),
	}
	return e
}

func (tm *TaskMaster) DelFork(name string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	fork := tm.forks[name]
	delete(tm.forks, name)
	if fork.Edge != nil {
		fork.Edge.Close()
	}
}
