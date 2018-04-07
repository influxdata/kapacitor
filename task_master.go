package kapacitor

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	imodels "github.com/influxdata/influxdb/models"
	"github.com/influxdata/kapacitor/alert"
	"github.com/influxdata/kapacitor/command"
	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/expvar"
	"github.com/influxdata/kapacitor/influxdb"
	"github.com/influxdata/kapacitor/keyvalue"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/server/vars"
	alertservice "github.com/influxdata/kapacitor/services/alert"
	"github.com/influxdata/kapacitor/services/alerta"
	ec2 "github.com/influxdata/kapacitor/services/ec2/client"
	"github.com/influxdata/kapacitor/services/hipchat"
	"github.com/influxdata/kapacitor/services/httpd"
	"github.com/influxdata/kapacitor/services/httppost"
	k8s "github.com/influxdata/kapacitor/services/k8s/client"
	"github.com/influxdata/kapacitor/services/kafka"
	"github.com/influxdata/kapacitor/services/mqtt"
	"github.com/influxdata/kapacitor/services/opsgenie"
	"github.com/influxdata/kapacitor/services/opsgenie2"
	"github.com/influxdata/kapacitor/services/pagerduty"
	"github.com/influxdata/kapacitor/services/pagerduty2"
	"github.com/influxdata/kapacitor/services/pushover"
	"github.com/influxdata/kapacitor/services/sensu"
	"github.com/influxdata/kapacitor/services/sideload"
	"github.com/influxdata/kapacitor/services/slack"
	"github.com/influxdata/kapacitor/services/smtp"
	"github.com/influxdata/kapacitor/services/snmptrap"
	swarm "github.com/influxdata/kapacitor/services/swarm/client"
	"github.com/influxdata/kapacitor/services/telegram"
	"github.com/influxdata/kapacitor/services/victorops"
	"github.com/influxdata/kapacitor/tick"
	"github.com/influxdata/kapacitor/tick/stateful"
	"github.com/influxdata/kapacitor/timer"
	"github.com/influxdata/kapacitor/udf"
)

const (
	statPointsReceived = "points_received"
	MainTaskMaster     = "main"
)

type LogService interface {
	NewLogger(prefix string, flag int) *log.Logger
}

type Diagnostic interface {
	WithTaskContext(task string) TaskDiagnostic
	WithTaskMasterContext(tm string) Diagnostic
	WithNodeContext(node string) NodeDiagnostic
	WithEdgeContext(task, parent, child string) EdgeDiagnostic

	TaskMasterOpened()
	TaskMasterClosed()

	StartingTask(id string)
	StartedTask(id string)

	StoppedTask(id string)
	StoppedTaskWithError(id string, err error)

	TaskMasterDot(d string)
}

type UDFService interface {
	List() []string
	Info(name string) (udf.Info, bool)
	Create(name, taskID, nodeID string, d udf.Diagnostic, abortCallback func()) (udf.Interface, error)
}

var ErrTaskMasterClosed = errors.New("TaskMaster is closed")
var ErrTaskMasterOpen = errors.New("TaskMaster is open")

type deleteHook func(*TaskMaster)

// An execution framework for  a set of tasks.
type TaskMaster struct {
	// Unique id for this task master instance
	id string

	ServerInfo vars.Infoer

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

	AlertService interface {
		alertservice.AnonHandlerRegistrar
		alertservice.Events
		alertservice.TopicPersister
		alertservice.InhibitorLookup
	}
	InfluxDBService interface {
		NewNamedClient(name string) (influxdb.Client, error)
	}
	SMTPService interface {
		Global() bool
		StateChangesOnly() bool
		Handler(smtp.HandlerConfig, ...keyvalue.T) alert.Handler
	}
	MQTTService interface {
		Handler(mqtt.HandlerConfig, ...keyvalue.T) alert.Handler
	}

	OpsGenieService interface {
		Global() bool
		Handler(opsgenie.HandlerConfig, ...keyvalue.T) alert.Handler
	}
	OpsGenie2Service interface {
		Global() bool
		Handler(opsgenie2.HandlerConfig, ...keyvalue.T) alert.Handler
	}
	VictorOpsService interface {
		Global() bool
		Handler(victorops.HandlerConfig, ...keyvalue.T) alert.Handler
	}
	PagerDutyService interface {
		Global() bool
		Handler(pagerduty.HandlerConfig, ...keyvalue.T) alert.Handler
	}
	PagerDuty2Service interface {
		Global() bool
		Handler(pagerduty2.HandlerConfig, ...keyvalue.T) alert.Handler
	}
	PushoverService interface {
		Handler(pushover.HandlerConfig, ...keyvalue.T) alert.Handler
	}
	HTTPPostService interface {
		Handler(httppost.HandlerConfig, ...keyvalue.T) alert.Handler
		Endpoint(string) (*httppost.Endpoint, bool)
	}
	SlackService interface {
		Global() bool
		StateChangesOnly() bool
		Handler(slack.HandlerConfig, ...keyvalue.T) alert.Handler
	}
	SNMPTrapService interface {
		Handler(snmptrap.HandlerConfig, ...keyvalue.T) (alert.Handler, error)
	}
	TelegramService interface {
		Global() bool
		StateChangesOnly() bool
		Handler(telegram.HandlerConfig, ...keyvalue.T) alert.Handler
	}
	HipChatService interface {
		Global() bool
		StateChangesOnly() bool
		Handler(hipchat.HandlerConfig, ...keyvalue.T) alert.Handler
	}
	KafkaService interface {
		Handler(kafka.HandlerConfig, ...keyvalue.T) (alert.Handler, error)
	}
	AlertaService interface {
		DefaultHandlerConfig() alerta.HandlerConfig
		Handler(alerta.HandlerConfig, ...keyvalue.T) (alert.Handler, error)
	}
	SensuService interface {
		Handler(sensu.HandlerConfig, ...keyvalue.T) (alert.Handler, error)
	}
	TalkService interface {
		Handler(...keyvalue.T) alert.Handler
	}
	TimingService interface {
		NewTimer(timer.Setter) timer.Timer
	}
	K8sService interface {
		Client(string) (k8s.Client, error)
	}
	SwarmService interface {
		Client(string) (swarm.Client, error)
	}
	EC2Service interface {
		Client(string) (ec2.Client, error)
	}

	SideloadService interface {
		Source(dir string) (sideload.Source, error)
	}

	Commander command.Commander

	DefaultRetentionPolicy string

	// Incoming streams
	writePointsIn StreamCollector
	writesClosed  bool
	writesMu      sync.RWMutex

	// Forks of incoming streams
	// We are mapping from (db, rp, measurement) to map of task ids to their edges
	// The outer map (from dbrp&measurement) is for fast access on forkPoint
	// While the inner map is for handling fork deletions better (see taskToForkKeys)
	forks map[forkKey]map[string]edge.Edge

	// Stats for number of points each fork has received
	forkStats map[forkKey]*expvar.Int

	// Task to fork keys is map to help in deletes, in deletes
	// we have only the task id, and they are called after the task is deleted from TaskMaster.tasks
	taskToForkKeys map[string][]forkKey

	// Set of incoming batches
	batches map[string][]BatchCollector

	// Executing tasks
	tasks map[string]*ExecutingTask

	// DeleteHooks for tasks
	deleteHooks map[string][]deleteHook

	diag Diagnostic

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
func NewTaskMaster(id string, info vars.Infoer, d Diagnostic) *TaskMaster {
	return &TaskMaster{
		id:             id,
		forks:          make(map[forkKey]map[string]edge.Edge),
		forkStats:      make(map[forkKey]*expvar.Int),
		taskToForkKeys: make(map[string][]forkKey),
		batches:        make(map[string][]BatchCollector),
		tasks:          make(map[string]*ExecutingTask),
		deleteHooks:    make(map[string][]deleteHook),
		ServerInfo:     info,
		diag:           d.WithTaskMasterContext(id),

		closed:        true,
		TimingService: noOpTimingService{},
	}
}

// Returns a new TaskMaster instance with the same services as the current one.
func (tm *TaskMaster) New(id string) *TaskMaster {
	n := NewTaskMaster(id, tm.ServerInfo, tm.diag)
	n.DefaultRetentionPolicy = tm.DefaultRetentionPolicy
	n.HTTPDService = tm.HTTPDService
	n.TaskStore = tm.TaskStore
	n.DeadmanService = tm.DeadmanService
	n.UDFService = tm.UDFService
	n.AlertService = tm.AlertService
	n.InfluxDBService = tm.InfluxDBService
	n.SMTPService = tm.SMTPService
	n.MQTTService = tm.MQTTService
	n.OpsGenieService = tm.OpsGenieService
	n.OpsGenie2Service = tm.OpsGenie2Service
	n.VictorOpsService = tm.VictorOpsService
	n.PagerDutyService = tm.PagerDutyService
	n.PagerDuty2Service = tm.PagerDuty2Service
	n.PushoverService = tm.PushoverService
	n.HTTPPostService = tm.HTTPPostService
	n.SlackService = tm.SlackService
	n.TelegramService = tm.TelegramService
	n.SNMPTrapService = tm.SNMPTrapService
	n.HipChatService = tm.HipChatService
	n.AlertaService = tm.AlertaService
	n.SensuService = tm.SensuService
	n.TalkService = tm.TalkService
	n.TimingService = tm.TimingService
	n.K8sService = tm.K8sService
	n.Commander = tm.Commander
	n.SideloadService = tm.SideloadService
	return n
}

func (tm *TaskMaster) ID() string {
	return tm.id
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
	tm.diag.TaskMasterOpened()
	return
}

func (tm *TaskMaster) StopTasks() {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	for _, et := range tm.tasks {
		_ = tm.stopTask(et.Task.ID)
	}
}

func (tm *TaskMaster) Close() error {
	tm.mu.Lock()
	closed := tm.closed
	tm.mu.Unlock()

	if closed {
		return ErrTaskMasterClosed
	}

	tm.Drain()

	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.closed = true
	for _, et := range tm.tasks {
		_ = tm.stopTask(et.Task.ID)
	}
	tm.diag.TaskMasterClosed()
	return nil
}

func (tm *TaskMaster) Drain() {
	tm.waitForForks()
	tm.mu.Lock()
	defer tm.mu.Unlock()

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
	// A task will always have a stream or batch node.
	// If it doesn't have anything more then the task does nothing with the data.
	if p.Len() <= 1 {
		return nil, fmt.Errorf("task does nothing")
	}
	t.Pipeline = p
	return t, nil
}

func (tm *TaskMaster) waitForForks() {
	tm.mu.Lock()
	drained := tm.drained
	tm.mu.Unlock()

	if drained {
		return
	}

	tm.mu.Lock()
	tm.drained = true
	tm.mu.Unlock()

	tm.writesMu.Lock()
	tm.writesClosed = true
	tm.writesMu.Unlock()

	// Close the write points in stream
	tm.writePointsIn.Close()

	// Don't hold the lock while we wait
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
				func(self interface{}, args ...interface{}) (interface{}, error) {
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
				},
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
	if len(t.DBRPs) == 0 {
		return nil, errors.New("task does contain any dbrps")
	}
	tm.diag.StartingTask(t.ID)
	et, err := NewExecutingTask(tm, t)
	if err != nil {
		return nil, err
	}

	var ins []edge.StatsEdge
	switch et.Task.Type {
	case StreamTask:
		e, err := tm.newFork(et.Task.ID, et.Task.DBRPs, et.Task.Measurements())
		if err != nil {
			return nil, err
		}
		ins = []edge.StatsEdge{e}
	case BatchTask:
		count, err := et.BatchCount()
		if err != nil {
			return nil, err
		}
		ins = make([]edge.StatsEdge, count)
		for i := 0; i < count; i++ {
			d := tm.diag.WithEdgeContext(t.ID, "batch", fmt.Sprintf("batch%d", i))
			in := newEdge(t.ID, "batch", fmt.Sprintf("batch%d", i), pipeline.BatchEdge, defaultEdgeBufferSize, d)
			ins[i] = in
			tm.batches[t.ID] = append(tm.batches[t.ID], &batchCollector{edge: in})
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
	tm.diag.StartedTask(t.ID)
	tm.diag.TaskMasterDot(string(t.Dot()))

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

func (tm *TaskMaster) DeleteTask(id string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if err := tm.stopTask(id); err != nil {
		return err
	}
	tm.deleteTask(id)
	return nil
}

// internal stopTask function. The caller must have acquired
// the lock in order to call this function
func (tm *TaskMaster) stopTask(id string) (err error) {
	if et, ok := tm.tasks[id]; ok {

		delete(tm.tasks, id)

		switch et.Task.Type {
		case StreamTask:
			tm.delFork(id)
		case BatchTask:
			delete(tm.batches, id)
		}

		err = et.stop()
		if err != nil {
			tm.diag.StoppedTaskWithError(id, err)
		} else {
			tm.diag.StoppedTask(id)
		}
	}
	return
}

// internal deleteTask function. The caller must have acquired
// the lock in order to call this function
func (tm *TaskMaster) deleteTask(id string) {
	hooks := tm.deleteHooks[id]
	for _, deleteHook := range hooks {
		deleteHook(tm)
	}
}

func (tm *TaskMaster) registerDeleteHookForTask(id string, hook deleteHook) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.deleteHooks[id] = append(tm.deleteHooks[id], hook)
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
	d := tm.diag.WithEdgeContext(fmt.Sprintf("task_master:%s", tm.id), name, "stream")
	in := newEdge(fmt.Sprintf("task_master:%s", tm.id), name, "stream", pipeline.StreamEdge, defaultEdgeBufferSize, d)
	se := &streamEdge{edge: in}
	tm.wg.Add(1)
	go func() {
		defer tm.wg.Done()
		tm.runForking(se)
	}()
	return se, nil
}

type StreamCollector interface {
	CollectPoint(edge.PointMessage) error
	Close() error
}

type StreamEdge interface {
	CollectPoint(edge.PointMessage) error
	EmitPoint() (edge.PointMessage, bool)
	Close() error
}

type streamEdge struct {
	edge edge.Edge
}

func (s *streamEdge) CollectPoint(p edge.PointMessage) error {
	return s.edge.Collect(p)
}
func (s *streamEdge) EmitPoint() (edge.PointMessage, bool) {
	m, ok := s.edge.Emit()
	if !ok {
		return nil, false
	}
	p, ok := m.(edge.PointMessage)
	if !ok {
		panic("impossible to receive non PointMessage message")
	}
	return p, true
}
func (s *streamEdge) Close() error {
	return s.edge.Close()
}

func (tm *TaskMaster) runForking(in StreamEdge) {
	for p, ok := in.EmitPoint(); ok; p, ok = in.EmitPoint() {
		tm.forkPoint(p)
	}
}

func (tm *TaskMaster) forkPoint(p edge.PointMessage) {
	tm.mu.RLock()
	locked := true
	defer func() {
		if locked {
			tm.mu.RUnlock()
		}
	}()

	// Create the fork keys - which is (db, rp, measurement)
	key := forkKey{
		Database:        p.Database(),
		RetentionPolicy: p.RetentionPolicy(),
		Measurement:     p.Name(),
	}

	// If we have empty measurement in this db,rp we need to send it all
	// the points
	emptyMeasurementKey := forkKey{
		Database:        p.Database(),
		RetentionPolicy: p.RetentionPolicy(),
		Measurement:     "",
	}

	// Merge the results to the forks map
	for _, edge := range tm.forks[key] {
		_ = edge.Collect(p)
	}

	for _, edge := range tm.forks[emptyMeasurementKey] {
		_ = edge.Collect(p)
	}

	c, ok := tm.forkStats[key]
	if !ok {
		// Release read lock
		tm.mu.RUnlock()
		locked = false

		// Get write lock
		tm.mu.Lock()
		// Now with write lock check again
		c, ok = tm.forkStats[key]
		if !ok {
			// Create statistics
			c = &expvar.Int{}
			tm.forkStats[key] = c
		}
		tm.mu.Unlock()

		tags := map[string]string{
			"task_master":      tm.id,
			"database":         key.Database,
			"retention_policy": key.RetentionPolicy,
			"measurement":      key.Measurement,
		}
		_, statMap := vars.NewStatistic("ingress", tags)
		statMap.Set(statPointsReceived, c)
	}
	c.Add(1)
}

func (tm *TaskMaster) WritePoints(database, retentionPolicy string, consistencyLevel imodels.ConsistencyLevel, points []imodels.Point) error {
	tm.writesMu.RLock()
	defer tm.writesMu.RUnlock()
	if tm.writesClosed {
		return ErrTaskMasterClosed
	}
	if retentionPolicy == "" {
		retentionPolicy = tm.DefaultRetentionPolicy
	}
	for _, mp := range points {
		p := edge.NewPointMessage(
			mp.Name(),
			database,
			retentionPolicy,
			models.Dimensions{},
			models.Fields(mp.Fields()),
			models.Tags(mp.Tags().Map()),
			mp.Time(),
		)
		err := tm.writePointsIn.CollectPoint(p)
		if err != nil {
			return err
		}
	}
	return nil
}

func (tm *TaskMaster) WriteKapacitorPoint(p edge.PointMessage) error {
	tm.writesMu.RLock()
	defer tm.writesMu.RUnlock()
	if tm.writesClosed {
		return ErrTaskMasterClosed
	}
	p = p.ShallowCopy()
	p.SetDimensions(models.Dimensions{})
	return tm.writePointsIn.CollectPoint(p)
}

func (tm *TaskMaster) NewFork(taskName string, dbrps []DBRP, measurements []string) (edge.StatsEdge, error) {
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
func (tm *TaskMaster) newFork(taskName string, dbrps []DBRP, measurements []string) (edge.StatsEdge, error) {
	if tm.closed {
		return nil, ErrTaskMasterClosed
	}

	d := tm.diag.WithEdgeContext(taskName, "stream", "stream0")
	e := newEdge(taskName, "stream", "stream0", pipeline.StreamEdge, defaultEdgeBufferSize, d)

	for _, key := range forkKeys(dbrps, measurements) {
		tm.taskToForkKeys[taskName] = append(tm.taskToForkKeys[taskName], key)

		// Add the task to the tasksMap if it doesn't exists
		tasksMap, ok := tm.forks[key]
		if !ok {
			tasksMap = make(map[string]edge.Edge, 0)
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

type TaskMasterLookup struct {
	sync.Mutex
	taskMasters map[string]*TaskMaster
}

func NewTaskMasterLookup() *TaskMasterLookup {
	return &TaskMasterLookup{
		taskMasters: make(map[string]*TaskMaster),
	}
}

func (tml *TaskMasterLookup) Get(id string) *TaskMaster {
	tml.Lock()
	defer tml.Unlock()
	return tml.taskMasters[id]
}

func (tml *TaskMasterLookup) Main() *TaskMaster {
	return tml.Get(MainTaskMaster)
}

func (tml *TaskMasterLookup) Set(tm *TaskMaster) {
	tml.Lock()
	defer tml.Unlock()
	tml.taskMasters[tm.id] = tm
}

func (tml *TaskMasterLookup) Delete(tm *TaskMaster) {
	tml.Lock()
	defer tml.Unlock()
	delete(tml.taskMasters, tm.id)
}

type BatchCollector interface {
	CollectBatch(edge.BufferedBatchMessage) error
	Close() error
}

type batchCollector struct {
	edge edge.Edge
}

func (c *batchCollector) CollectBatch(batch edge.BufferedBatchMessage) error {
	return c.edge.Collect(batch)
}
func (c *batchCollector) Close() error {
	return c.edge.Close()
}
