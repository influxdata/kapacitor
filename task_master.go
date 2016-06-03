package kapacitor

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	client "github.com/influxdata/influxdb/client/v2"
	imodels "github.com/influxdata/influxdb/models"
	"github.com/influxdata/kapacitor/expvar"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/services/httpd"
	"github.com/influxdata/kapacitor/tick"
	"github.com/influxdata/kapacitor/tick/stateful"
	"github.com/influxdata/kapacitor/timer"
	"github.com/influxdata/kapacitor/udf"
)

const (
	statPointsReceived = "points_received"
)

type LogService interface {
	NewLogger(prefix string, flag int) *log.Logger
}

type UDFService interface {
	List() []string
	Info(name string) (udf.Info, bool)
	Create(name string, l *log.Logger, abortCallback func()) (udf.Interface, error)
}

var ErrTaskMasterClosed = errors.New("TaskMaster is closed")
var ErrTaskMasterOpen = errors.New("TaskMaster is open")

// An execution framework for  a set of tasks.
type TaskMaster struct {
	// Unique name for this task master instance
	name string

	HTTPDService interface {
		AddRoutes([]httpd.Route) error
		DelRoutes([]httpd.Route)
		URL() string
	}
	TaskStore interface {
		SaveSnapshot(id string, snapshot *TaskSnapshot) error
		HasSnapshot(id string) bool
		LoadSnapshot(id string) (*TaskSnapshot, error)
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
		Alert(serviceKey, incidentKey, desc string, level AlertLevel, details interface{}) error
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
	// We are mapping from (db, rp, measurement) to map of task ids to their edges
	// The outer map (from dbrp&measurement) is for fast access on forkPoint
	// While the inner map is for handling fork deletions better (see taskToForkKeys)
	forks map[forkKey]map[string]*Edge

	// Stats for number of points each fork has received
	forkStats map[forkKey]*expvar.Int

	// Task to fork keys is map to help in deletes, in deletes
	// we have only the task id, and they are called after the task is deleted from TaskMaster.tasks
	taskToForkKeys map[string][]forkKey

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

type forkKey struct {
	Database        string
	RetentionPolicy string
	Measurement     string
}

// Create a new Executor with a given clock.
func NewTaskMaster(name string, l LogService) *TaskMaster {
	return &TaskMaster{
		name:           name,
		forks:          make(map[forkKey]map[string]*Edge),
		forkStats:      make(map[forkKey]*expvar.Int),
		taskToForkKeys: make(map[string][]forkKey),
		batches:        make(map[string][]BatchCollector),
		tasks:          make(map[string]*ExecutingTask),
		LogService:     l,
		logger:         l.NewLogger(fmt.Sprintf("[task_master:%s] ", name), log.LstdFlags),
		closed:         true,
		TimingService:  noOpTimingService{},
	}
}

// Returns a new TaskMaster instance with the same services as the current one.
func (tm *TaskMaster) New(name string) *TaskMaster {
	n := NewTaskMaster(name, tm.LogService)
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
		tm.stopTask(et.Task.ID)
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
		tm.stopTask(et.Task.ID)
	}
	tm.logger.Println("I! closed")
	return nil
}

func (tm *TaskMaster) Drain() {
	tm.waitForForks()
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// TODO(yosia): handle this thing ;)
	for id, _ := range tm.taskToForkKeys {
		tm.delFork(id)
	}
}

// Create a new template in the context of a TaskMaster
func (tm *TaskMaster) NewTemplate(
	id,
	script string,
	tt TaskType,
) (*Template, error) {
	t := &Template{
		id: id,
	}
	scope := tm.CreateTICKScope()

	var srcEdge pipeline.EdgeType
	switch tt {
	case StreamTask:
		srcEdge = pipeline.StreamEdge
	case BatchTask:
		srcEdge = pipeline.BatchEdge
	}

	tp, err := pipeline.CreateTemplatePipeline(script, srcEdge, scope, tm.DeadmanService)
	if err != nil {
		return nil, err
	}
	t.tp = tp
	return t, nil
}

// Create a new task in the context of a TaskMaster
func (tm *TaskMaster) NewTask(
	id,
	script string,
	tt TaskType,
	dbrps []DBRP,
	snapshotInterval time.Duration,
	vars map[string]tick.Var,
) (*Task, error) {
	t := &Task{
		ID:               id,
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

	p, err := pipeline.CreatePipeline(script, srcEdge, scope, tm.DeadmanService, vars)
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

func (tm *TaskMaster) CreateTICKScope() *stateful.Scope {
	scope := stateful.NewScope()
	scope.Set("time", groupByTime)
	// Add dynamic methods to the scope for UDFs
	if tm.UDFService != nil {
		for _, f := range tm.UDFService.List() {
			f := f
			info, _ := tm.UDFService.Info(f)
			scope.SetDynamicMethod(
				f,
				stateful.DynamicMethod(func(self interface{}, args ...interface{}) (interface{}, error) {
					parent, ok := self.(pipeline.Node)
					if !ok {
						return nil, fmt.Errorf("cannot call %s on %T", f, self)
					}
					udf := pipeline.NewUDF(
						parent,
						f,
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
	tm.logger.Println("D! Starting task:", t.ID)
	et, err := NewExecutingTask(tm, t)
	if err != nil {
		return nil, err
	}

	var ins []*Edge
	switch et.Task.Type {
	case StreamTask:
		e, err := tm.newFork(et.Task.ID, et.Task.DBRPs, et.Task.Measurements())
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
			in := newEdge(t.ID, "batch", fmt.Sprintf("batch%d", i), pipeline.BatchEdge, defaultEdgeBufferSize, tm.LogService)
			ins[i] = in
			tm.batches[t.ID] = append(tm.batches[t.ID], in)
		}
	}

	var snapshot *TaskSnapshot
	if tm.TaskStore.HasSnapshot(t.ID) {
		snapshot, err = tm.TaskStore.LoadSnapshot(t.ID)
		if err != nil {
			return nil, err
		}
	}

	err = et.start(ins, snapshot)
	if err != nil {
		return nil, err
	}

	tm.tasks[et.Task.ID] = et
	tm.logger.Println("I! Started task:", t.ID)
	tm.logger.Println("D!", string(t.Dot()))

	return et, nil
}

func (tm *TaskMaster) BatchCollectors(id string) []BatchCollector {
	return tm.batches[id]
}

func (tm *TaskMaster) StopTask(id string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	return tm.stopTask(id)
}

// internal stopTask function. The caller must have acquired
// the lock in order to call this function
func (tm *TaskMaster) stopTask(id string) (err error) {
	if et, ok := tm.tasks[id]; ok {
		delete(tm.tasks, id)
		if et.Task.Type == StreamTask {
			tm.delFork(id)
		}
		err = et.stop()
		if err != nil {
			tm.logger.Println("E! Stopped task:", id, err)
		} else {
			tm.logger.Println("I! Stopped task:", id)
		}
	}
	return
}

func (tm *TaskMaster) IsExecuting(id string) bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	_, executing := tm.tasks[id]
	return executing
}

func (tm *TaskMaster) ExecutionStats(id string) (ExecutionStats, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	task, executing := tm.tasks[id]
	if !executing {
		return ExecutionStats{}, nil
	}

	return task.ExecutionStats()
}

func (tm *TaskMaster) ExecutingDot(id string, labels bool) string {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	et, executing := tm.tasks[id]
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
	in := newEdge(fmt.Sprintf("task_master:%s", tm.name), name, "stream", pipeline.StreamEdge, defaultEdgeBufferSize, tm.LogService)
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

	// Create the fork keys - which is (db, rp, measurement)
	key := forkKey{
		Database:        p.Database,
		RetentionPolicy: p.RetentionPolicy,
		Measurement:     p.Name,
	}

	// If we have empty measurement in this db,rp we need to send it all
	// the points
	emptyMeasurementKey := forkKey{
		Database:        p.Database,
		RetentionPolicy: p.RetentionPolicy,
		Measurement:     "",
	}

	// Merge the results to the forks map
	for _, edge := range tm.forks[key] {
		edge.CollectPoint(p)
	}

	for _, edge := range tm.forks[emptyMeasurementKey] {
		edge.CollectPoint(p)
	}

	c, ok := tm.forkStats[key]
	if !ok {
		// Create statistics
		c = &expvar.Int{}
		tm.forkStats[key] = c

		tags := map[string]string{
			"task_master":      tm.name,
			"database":         key.Database,
			"retention_policy": key.RetentionPolicy,
			"measurement":      key.Measurement,
		}
		_, statMap := NewStatistics("ingress", tags)
		statMap.Set(statPointsReceived, c)
	}
	c.Add(1)
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

func (tm *TaskMaster) NewFork(taskName string, dbrps []DBRP, measurements []string) (*Edge, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	return tm.newFork(taskName, dbrps, measurements)
}

func forkKeys(dbrps []DBRP, measurements []string) []forkKey {
	keys := make([]forkKey, 0)

	for _, dbrp := range dbrps {
		for _, measurement := range measurements {
			key := forkKey{
				RetentionPolicy: dbrp.RetentionPolicy,
				Database:        dbrp.Database,
				Measurement:     measurement,
			}

			keys = append(keys, key)
		}
	}

	return keys
}

// internal newFork, must have acquired lock before calling.
func (tm *TaskMaster) newFork(taskName string, dbrps []DBRP, measurements []string) (*Edge, error) {
	if tm.closed {
		return nil, ErrTaskMasterClosed
	}

	e := newEdge(taskName, "stream", "stream0", pipeline.StreamEdge, defaultEdgeBufferSize, tm.LogService)

	for _, key := range forkKeys(dbrps, measurements) {
		tm.taskToForkKeys[taskName] = append(tm.taskToForkKeys[taskName], key)

		// Add the task to the tasksMap if it doesn't exists
		tasksMap, ok := tm.forks[key]
		if !ok {
			tasksMap = make(map[string]*Edge, 0)
		}

		// Add the edge to task map
		tasksMap[taskName] = e

		// update the task map in the forks
		tm.forks[key] = tasksMap
	}

	return e, nil
}

func (tm *TaskMaster) DelFork(id string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.delFork(id)
}

// internal delFork function, must have lock to call
func (tm *TaskMaster) delFork(id string) {

	// mark if we already closed the edge because the edge is replicated
	// by it's fork keys (db,rp,measurement)
	isEdgeClosed := false

	// Find the fork keys
	for _, key := range tm.taskToForkKeys[id] {

		// check if the edge exists
		edge, ok := tm.forks[key][id]
		if ok {

			// Only close the edge if we are already didn't closed it
			if edge != nil && !isEdgeClosed {
				isEdgeClosed = true
				edge.Close()
			}

			// remove the task in fork map
			delete(tm.forks[key], id)
		}
	}

	// remove mapping from task id to it's keys
	delete(tm.taskToForkKeys, id)
}

func (tm *TaskMaster) SnapshotTask(id string) (*TaskSnapshot, error) {
	tm.mu.RLock()
	et, ok := tm.tasks[id]
	tm.mu.RUnlock()

	if ok {
		return et.Snapshot()
	}
	return nil, fmt.Errorf("task %s is not running or does not exist", id)
}

type noOpTimingService struct{}

func (noOpTimingService) NewTimer(timer.Setter) timer.Timer {
	return timer.NewNoOp()
}
