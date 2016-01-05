package kapacitor

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/influxdb/influxdb/client"
	"github.com/influxdb/influxdb/cluster"
	"github.com/influxdb/kapacitor/models"
	"github.com/influxdb/kapacitor/pipeline"
	"github.com/influxdb/kapacitor/services/httpd"
)

type LogService interface {
	NewLogger(prefix string, flag int) *log.Logger
}

var ErrTaskMasterClosed = errors.New("TaskMaster is closed")
var ErrTaskMasterOpen = errors.New("TaskMaster is open")

// An execution framework for  a set of tasks.
type TaskMaster struct {
	HTTPDService interface {
		AddRoutes([]httpd.Route) error
		DelRoutes([]httpd.Route)
		URL() string
	}
	InfluxDBService interface {
		NewClient() (*client.Client, error)
	}
	SMTPService interface {
		Global() bool
		SendMail(to []string, subject string, msg string) error
	}
	OpsGenieService interface {
		Global() bool
		Alert(teams []string, recipients []string, messageType, message, entityID string, t time.Time, details interface{}) error
	}
	VictorOpsService interface {
		Global() bool
		Alert(routingKey, messageType, message, entityID string, t time.Time, extra interface{}) error
	}
	PagerDutyService interface {
		Global() bool
		Alert(incidentKey, desc string, details interface{}) error
	}
	SlackService interface {
		Global() bool
		Alert(channel, message string, level AlertLevel) error
	}
	HipChatService interface {
		Global() bool
		Alert(room, token, message string, level AlertLevel) error
	}
	LogService LogService

	// Incoming streams
	writePointsIn StreamCollector
	// Forks of incoming streams
	forks map[string]fork

	// Set of incoming batches
	batches map[string][]BatchCollector

	// Executing tasks
	tasks map[string]*ExecutingTask

	logger *log.Logger

	closed  bool
	drained bool
	mu      sync.RWMutex
	wg      sync.WaitGroup
}

// A fork of the main data stream filtered by a set of DBRPs
type fork struct {
	Edge  *Edge
	dbrps map[DBRP]bool
}

// Create a new Executor with a given clock.
func NewTaskMaster(l LogService) *TaskMaster {
	return &TaskMaster{
		forks:      make(map[string]fork),
		batches:    make(map[string][]BatchCollector),
		tasks:      make(map[string]*ExecutingTask),
		LogService: l,
		logger:     l.NewLogger("[task_master] ", log.LstdFlags),
		closed:     true,
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

func (tm *TaskMaster) Open() (err error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if !tm.closed {
		return ErrTaskMasterOpen
	}
	tm.closed = false
	tm.drained = false
	tm.writePointsIn, err = tm.stream("write_points")
	if err != nil {
		tm.closed = true
		return
	}
	tm.logger.Println("I! opened")
	return
}

func (tm *TaskMaster) Close() error {
	tm.Drain()
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if tm.closed {
		return ErrTaskMasterClosed
	}
	tm.closed = true
	for _, et := range tm.tasks {
		tm.stopTask(et.Task.Name)
	}
	tm.logger.Println("I! closed")
	return nil
}

func (tm *TaskMaster) Drain() {
	tm.waitForForks()
	tm.mu.Lock()
	defer tm.mu.Unlock()
	for name := range tm.forks {
		tm.delFork(name)
	}
}

func (tm *TaskMaster) waitForForks() {
	if tm.drained {
		return
	}
	tm.drained = true
	tm.writePointsIn.Close()
	tm.wg.Wait()
}

func (tm *TaskMaster) StartTask(t *Task) (*ExecutingTask, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if tm.closed {
		return nil, errors.New("task master is closed cannot start a task")
	}
	tm.logger.Println("D! Starting task:", t.Name)
	et, err := NewExecutingTask(tm, t)
	if err != nil {
		return nil, err
	}

	var ins []*Edge
	switch et.Task.Type {
	case StreamTask:
		e, err := tm.newFork(et.Task.Name, et.Task.DBRPs)
		if err != nil {
			return nil, err
		}
		ins = []*Edge{e}
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

func (tm *TaskMaster) StopTask(name string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	return tm.stopTask(name)
}

// internal stopTask function. The caller must have acquired
// the lock in order to call this function
func (tm *TaskMaster) stopTask(name string) (err error) {
	if et, ok := tm.tasks[name]; ok {
		delete(tm.tasks, name)
		if et.Task.Type == StreamTask {
			tm.delFork(name)
		}
		err = et.stop()
		if err != nil {
			tm.logger.Println("E! Stopped task:", name, err)
		} else {
			tm.logger.Println("I! Stopped task:", name)
		}
	}
	return
}

func (tm *TaskMaster) IsExecuting(name string) bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	_, executing := tm.tasks[name]
	return executing
}

func (tm *TaskMaster) ExecutingDot(name string) string {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	et, executing := tm.tasks[name]
	if executing {
		return string(et.EDot())
	}
	return ""
}

func (tm *TaskMaster) Stream(name string) (StreamCollector, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	return tm.stream(name)
}

func (tm *TaskMaster) stream(name string) (StreamCollector, error) {
	if tm.closed {
		return nil, ErrTaskMasterClosed
	}
	in := newEdge("TASK_MASTER", name, "stream", pipeline.StreamEdge, tm.LogService)
	tm.drained = false
	tm.wg.Add(1)
	go tm.runForking(in)
	return in, nil
}

func (tm *TaskMaster) runForking(in *Edge) {
	defer tm.wg.Done()
	for p, ok := in.NextPoint(); ok; p, ok = in.NextPoint() {
		tm.forkPoint(p)
	}
}

func (tm *TaskMaster) forkPoint(p models.Point) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	for _, fork := range tm.forks {
		dbrp := DBRP{
			Database:        p.Database,
			RetentionPolicy: p.RetentionPolicy,
		}
		if fork.dbrps[dbrp] {
			fork.Edge.CollectPoint(p)
		}
	}
}

func (tm *TaskMaster) WritePoints(pts *cluster.WritePointsRequest) error {
	if tm.closed {
		return ErrTaskMasterClosed
	}
	for _, mp := range pts.Points {
		p := models.Point{
			Database:        pts.Database,
			RetentionPolicy: pts.RetentionPolicy,
			Name:            mp.Name(),
			Group:           models.NilGroup,
			Tags:            models.Tags(mp.Tags()),
			Fields:          models.Fields(mp.Fields()),
			Time:            mp.Time(),
		}
		err := tm.writePointsIn.CollectPoint(p)
		if err != nil {
			return err
		}
	}
	return nil
}

func (tm *TaskMaster) NewFork(taskName string, dbrps []DBRP) (*Edge, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	return tm.newFork(taskName, dbrps)
}

// internal newFork, must have acquired lock before calling.
func (tm *TaskMaster) newFork(taskName string, dbrps []DBRP) (*Edge, error) {
	if tm.closed {
		return nil, ErrTaskMasterClosed
	}
	e := newEdge(taskName, "stream", "stream0", pipeline.StreamEdge, tm.LogService)
	tm.forks[taskName] = fork{
		Edge:  e,
		dbrps: CreateDBRPMap(dbrps),
	}
	return e, nil
}

func (tm *TaskMaster) DelFork(name string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.delFork(name)
}

// internal delFork function, must have lock to call
func (tm *TaskMaster) delFork(name string) {
	fork := tm.forks[name]
	delete(tm.forks, name)
	if fork.Edge != nil {
		fork.Edge.Close()
	}
}
