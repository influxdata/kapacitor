package kapacitor

import (
	"log"
	"net"
	"os"

	"github.com/influxdb/influxdb/client"
	"github.com/influxdb/kapacitor/pipeline"
	"github.com/influxdb/kapacitor/services/httpd"
	"github.com/influxdb/kapacitor/wlog"
)

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

	// Incoming stream and forks
	in    *Edge
	forks map[string]fork

	// Set on incoming batches
	batches map[string]*Edge

	// Executing tasks
	tasks map[string]*ExecutingTask

	logger *log.Logger
}

// A fork of the main data stream filtered by a set of DBRPs
type fork struct {
	Edge  *Edge
	dbrps map[DBRP]bool
}

// Create a new Executor with a given clock.
func NewTaskMaster() *TaskMaster {
	src := newEdge("src->stream", pipeline.StreamEdge)
	return &TaskMaster{
		Stream:  src,
		in:      src,
		forks:   make(map[string]fork),
		batches: make(map[string]*Edge),
		tasks:   make(map[string]*ExecutingTask),
		logger:  wlog.New(os.Stderr, "[task_master] ", log.LstdFlags),
	}
}

// Returns a new TaskMaster instance with the same services as the current one.
func (tm *TaskMaster) New() *TaskMaster {
	n := NewTaskMaster()
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
	et := NewExecutingTask(tm, t)

	var in *Edge
	switch et.Task.Type {
	case StreamerTask:
		in = tm.NewFork(et.Task.Name, et.Task.DBRPs)
	case BatcherTask:
		in = newEdge("batch->"+et.Task.Name, pipeline.BatchEdge)
		tm.batches[t.Name] = in
	}

	err := et.start(in)
	if err != nil {
		return nil, err
	}

	tm.tasks[et.Task.Name] = et
	tm.logger.Println("I! Started task:", t.Name)

	return et, nil
}

func (tm *TaskMaster) BatchCollector(name string) BatchCollector {
	return tm.batches[name]
}

func (tm *TaskMaster) StopTask(name string) {
	if et, ok := tm.tasks[name]; ok {
		delete(tm.tasks, name)
		if et.Task.Type == StreamerTask {
			tm.DelFork(et.Task.Name)
		}
		et.stop()
		tm.logger.Println("I! Stopped task:", name)
	}
}

func (tm *TaskMaster) runForking() {
	for p, ok := tm.in.NextPoint(); ok; p, ok = tm.in.NextPoint() {
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
	for _, fork := range tm.forks {
		fork.Edge.Close()
	}
}

func (tm *TaskMaster) NewFork(name string, dbrps []DBRP) *Edge {
	short := name
	if len(short) > 8 {
		short = short[:8]
	}
	e := newEdge("stream->"+name, pipeline.StreamEdge)
	tm.forks[name] = fork{
		Edge:  e,
		dbrps: CreateDBRPMap(dbrps),
	}
	return e
}

func (tm *TaskMaster) DelFork(name string) {
	fork := tm.forks[name]
	delete(tm.forks, name)
	if fork.Edge != nil {
		fork.Edge.Close()
	}
}
