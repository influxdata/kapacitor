package kapacitor

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	client "github.com/influxdata/influxdb/client/v2"
	imodels "github.com/influxdata/influxdb/models"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/services/httpd"
	"github.com/influxdata/kapacitor/tick"
	"github.com/influxdata/kapacitor/timer"
)

type LogService interface {
	NewLogger(prefix string, flag int) *log.Logger
}
type UDFService interface {
	FunctionList() []string
	FunctionInfo(name string) (UDFProcessInfo, bool)
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
	TaskStore interface {
		SaveSnapshot(name string, snapshot *TaskSnapshot) error
		HasSnapshot(name string) bool
		LoadSnapshot(name string) (*TaskSnapshot, error)
	}
	DeadmanService pipeline.DeadmanService

	UDFService UDFService

	InfluxDBService interface {
		NewDefaultClient() (client.Client, error)
		NewNamedClient(name string) (client.Client, error)
	}
	SMTPService interface {
		Global() bool
		StateChangesOnly() bool
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
		Alert(incidentKey, desc string, level AlertLevel, details interface{}) error
	}
	SlackService interface {
		Global() bool
		StateChangesOnly() bool
		Alert(channel, message string, level AlertLevel) error
	}
	HipChatService interface {
		Global() bool
		StateChangesOnly() bool
		Alert(room, token, message string, level AlertLevel) error
	}
	AlertaService interface {
		Alert(token,
			resource,
			event,
			environment,
			severity,
			status,
			group,
			value,
			message,
			origin string,
			service []string,
			data interface{}) error
	}
	SensuService interface {
		Alert(name, output string, level AlertLevel) error
	}
	TalkService interface {
		Alert(title, text string) error
	}
	TimingService interface {
		NewTimer(timer.Setter) timer.Timer
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
		forks:         make(map[string]fork),
		batches:       make(map[string][]BatchCollector),
		tasks:         make(map[string]*ExecutingTask),
		LogService:    l,
		logger:        l.NewLogger("[task_master] ", log.LstdFlags),
		closed:        true,
		TimingService: noOpTimingService{},
	}
}

// Returns a new TaskMaster instance with the same services as the current one.
func (tm *TaskMaster) New() *TaskMaster {
	n := NewTaskMaster(tm.LogService)
	n.HTTPDService = tm.HTTPDService
	n.UDFService = tm.UDFService
	n.DeadmanService = tm.DeadmanService
	n.TaskStore = tm.TaskStore
	n.InfluxDBService = tm.InfluxDBService
	n.SMTPService = tm.SMTPService
	n.OpsGenieService = tm.OpsGenieService
	n.VictorOpsService = tm.VictorOpsService
	n.PagerDutyService = tm.PagerDutyService
	n.SlackService = tm.SlackService
	n.HipChatService = tm.HipChatService
	n.AlertaService = tm.AlertaService
	n.SensuService = tm.SensuService
	n.TalkService = tm.TalkService
	n.TimingService = tm.TimingService
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

func (tm *TaskMaster) StopTasks() {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	for _, et := range tm.tasks {
		tm.stopTask(et.Task.Name)
	}
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

// Create a new task in the context of a TaskMaster
func (tm *TaskMaster) NewTask(
	name,
	script string,
	tt TaskType,
	dbrps []DBRP,
	snapshotInterval time.Duration,
) (*Task, error) {
	t := &Task{
		Name:             name,
		Type:             tt,
		DBRPs:            dbrps,
		SnapshotInterval: snapshotInterval,
	}
	scope := tm.CreateTICKScope()

	var srcEdge pipeline.EdgeType
	switch tt {
	case StreamTask:
		srcEdge = pipeline.StreamEdge
	case BatchTask:
		srcEdge = pipeline.BatchEdge
	}

	p, err := pipeline.CreatePipeline(script, srcEdge, scope, tm.DeadmanService)
	if err != nil {
		return nil, err
	}
	t.Pipeline = p
	return t, nil
}

func (tm *TaskMaster) waitForForks() {
	if tm.drained {
		return
	}
	tm.drained = true
	tm.writePointsIn.Close()
	tm.wg.Wait()
}

func (tm *TaskMaster) CreateTICKScope() *tick.Scope {
	scope := tick.NewScope()
	scope.Set("time", func(d time.Duration) time.Duration { return d })
	// Add dynamic methods to the scope for UDFs
	if tm.UDFService != nil {
		for _, f := range tm.UDFService.FunctionList() {
			f := f
			info, _ := tm.UDFService.FunctionInfo(f)
			scope.SetDynamicMethod(
				f,
				tick.DynamicMethod(func(self interface{}, args ...interface{}) (interface{}, error) {
					parent, ok := self.(pipeline.Node)
					if !ok {
						return nil, fmt.Errorf("cannot call %s on %T", f, self)
					}
					udf := pipeline.NewUDF(
						parent,
						f,
						info.Commander,
						info.Timeout,
						info.Wants,
						info.Provides,
						info.Options,
					)
					return udf, nil
				}),
			)
		}
	}
	return scope
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
			in := newEdge(t.Name, "batch", fmt.Sprintf("batch%d", i), pipeline.BatchEdge, defaultEdgeBufferSize, tm.LogService)
			ins[i] = in
			tm.batches[t.Name] = append(tm.batches[t.Name], in)
		}
	}

	var snapshot *TaskSnapshot
	if tm.TaskStore.HasSnapshot(t.Name) {
		snapshot, err = tm.TaskStore.LoadSnapshot(t.Name)
		if err != nil {
			return nil, err
		}
	}

	err = et.start(ins, snapshot)
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

func (tm *TaskMaster) ExecutionStats(name string) (ExecutionStats, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	task, executing := tm.tasks[name]
	if !executing {
		return ExecutionStats{}, nil
	}

	return task.ExecutionStats()
}

func (tm *TaskMaster) ExecutingDot(name string, labels bool) string {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	et, executing := tm.tasks[name]
	if executing {
		return string(et.EDot(labels))
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
	in := newEdge("TASK_MASTER", name, "stream", pipeline.StreamEdge, defaultEdgeBufferSize, tm.LogService)
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

func (tm *TaskMaster) WritePoints(database, retentionPolicy string, consistencyLevel imodels.ConsistencyLevel, points []imodels.Point) error {
	if tm.closed {
		return ErrTaskMasterClosed
	}
	for _, mp := range points {
		p := models.Point{
			Database:        database,
			RetentionPolicy: retentionPolicy,
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
	e := newEdge(taskName, "stream", "srcstream0", pipeline.StreamEdge, defaultEdgeBufferSize, tm.LogService)
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

func (tm *TaskMaster) SnapshotTask(name string) (*TaskSnapshot, error) {
	tm.mu.RLock()
	et, ok := tm.tasks[name]
	tm.mu.RUnlock()

	if ok {
		return et.Snapshot()
	}
	return nil, fmt.Errorf("task %s is not running or does not exist", name)
}

type noOpTimingService struct{}

func (noOpTimingService) NewTimer(timer.Setter) timer.Timer {
	return timer.NewNoOp()
}
