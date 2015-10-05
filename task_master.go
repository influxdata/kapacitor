package kapacitor

import (
	"net"

	"github.com/influxdb/kapacitor/pipeline"
	"github.com/influxdb/kapacitor/services/httpd"
)

// An execution framework for  a set of tasks.
type TaskMaster struct {
	Stream       StreamCollector
	HTTPDService interface {
		AddRoutes([]httpd.Route) error
		DelRoutes([]httpd.Route)
		Addr() net.Addr
	}

	// Incoming stream and forks
	in    *Edge
	forks map[string]*Edge

	// Set on incoming batches
	batches map[string]*Edge

	// Executing tasks
	tasks map[string]*ExecutingTask
}

// Create a new Executor with a given clock.
func NewTaskMaster() *TaskMaster {
	src := newEdge("src->stream", pipeline.StreamEdge)
	return &TaskMaster{
		Stream:  src,
		in:      src,
		forks:   make(map[string]*Edge),
		batches: make(map[string]*Edge),
		tasks:   make(map[string]*ExecutingTask),
	}
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
		in = tm.NewFork(et.Task.Name)
	case BatcherTask:
		in = newEdge("batch->"+et.Task.Name, pipeline.BatchEdge)
		tm.batches[t.Name] = in
	}

	err := et.start(in)
	if err != nil {
		return nil, err
	}

	tm.tasks[et.Task.Name] = et

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
	}
}

func (tm *TaskMaster) runForking() {
	for p := tm.in.NextPoint(); p != nil; p = tm.in.NextPoint() {
		for _, out := range tm.forks {
			out.CollectPoint(p)
		}
	}
	for _, out := range tm.forks {
		out.Close()
	}
}

func (tm *TaskMaster) NewFork(name string) *Edge {
	short := name
	if len(short) > 8 {
		short = short[:8]
	}
	e := newEdge("stream->"+name, pipeline.StreamEdge)
	tm.forks[name] = e
	return e
}
func (tm *TaskMaster) DelFork(name string) {
	delete(tm.forks, name)
}
