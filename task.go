package kapacitor

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/influxdata/kapacitor/pipeline"
)

// The type of a task
type TaskType int

const (
	StreamTask TaskType = iota
	BatchTask
)

func (t TaskType) String() string {
	switch t {
	case StreamTask:
		return "stream"
	case BatchTask:
		return "batch"
	default:
		return "unknown"
	}
}

func (t TaskType) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (t *TaskType) UnmarshalText(text []byte) error {
	switch string(text) {
	case "stream":
		*t = StreamTask
	case "batch":
		*t = BatchTask
	default:
		return fmt.Errorf("unknown task type %s", string(text))
	}
	return nil
}

type DBRP struct {
	Database        string `json:"db"`
	RetentionPolicy string `json:"rp"`
}

func CreateDBRPMap(dbrps []DBRP) map[DBRP]bool {
	dbMap := make(map[DBRP]bool, len(dbrps))
	for _, dbrp := range dbrps {
		dbMap[dbrp] = true
	}
	return dbMap
}

func (d DBRP) String() string {
	return fmt.Sprintf("%q.%q", d.Database, d.RetentionPolicy)
}

// The complete definition of a task, its id, pipeline and type.
type Task struct {
	ID               string
	Pipeline         *pipeline.Pipeline
	Type             TaskType
	DBRPs            []DBRP
	SnapshotInterval time.Duration
}

func (t *Task) Dot() []byte {
	return t.Pipeline.Dot(t.ID)
}

// returns all the measurements from a FromNode
func (t *Task) Measurements() []string {
	measurements := make([]string, 0)

	_ = t.Pipeline.Walk(func(node pipeline.Node) error {
		switch streamNode := node.(type) {
		case *pipeline.FromNode:
			measurements = append(measurements, streamNode.Measurement)
		}
		return nil
	})

	return measurements
}

// ----------------------------------
// ExecutingTask

// A task that is ready for execution.
type ExecutingTask struct {
	tm      *TaskMaster
	Task    *Task
	source  Node
	outputs map[string]Output
	// node lookup from pipeline.ID -> Node
	lookup   map[pipeline.ID]Node
	nodes    []Node
	stopping chan struct{}
	wg       sync.WaitGroup
	logger   *log.Logger

	// Mutex for throughput var
	tmu        sync.RWMutex
	throughput float64
}

// Create a new  task from a defined kapacitor.
func NewExecutingTask(tm *TaskMaster, t *Task) (*ExecutingTask, error) {
	l := tm.LogService.NewLogger(fmt.Sprintf("[task:%s] ", t.ID), log.LstdFlags)
	et := &ExecutingTask{
		tm:      tm,
		Task:    t,
		outputs: make(map[string]Output),
		lookup:  make(map[pipeline.ID]Node),
		logger:  l,
	}
	err := et.link()
	if err != nil {
		return nil, err
	}
	return et, nil
}

// walks the entire pipeline applying function f.
func (et *ExecutingTask) walk(f func(n Node) error) error {
	for _, n := range et.nodes {
		err := f(n)
		if err != nil {
			return err
		}
	}
	return nil
}

// walks the entire pipeline in reverse order applying function f.
func (et *ExecutingTask) rwalk(f func(n Node) error) error {
	for i := len(et.nodes) - 1; i >= 0; i-- {
		err := f(et.nodes[i])
		if err != nil {
			return err
		}
	}
	return nil
}

// Link all the nodes together based on the task pipeline.
func (et *ExecutingTask) link() error {

	// Walk Pipeline and create equivalent executing nodes
	err := et.Task.Pipeline.Walk(func(n pipeline.Node) error {
		l := et.tm.LogService.NewLogger(
			fmt.Sprintf("[%s:%s] ", et.Task.ID, n.Name()),
			log.LstdFlags,
		)
		en, err := et.createNode(n, l)
		if err != nil {
			return err
		}
		et.lookup[n.ID()] = en
		// Save the walk order
		et.nodes = append(et.nodes, en)
		// Duplicate the Edges
		for _, p := range n.Parents() {
			ep := et.lookup[p.ID()]
			err := ep.linkChild(en)
			if err != nil {
				return err
			}
		}
		return err
	})
	if err != nil {
		return err
	}

	// The first node is always the source node
	et.source = et.nodes[0]
	return nil
}

// Start the task.
func (et *ExecutingTask) start(ins []*Edge, snapshot *TaskSnapshot) error {

	for _, in := range ins {
		et.source.addParentEdge(in)
	}
	validSnapshot := false
	if snapshot != nil {
		err := et.walk(func(n Node) error {
			_, ok := snapshot.NodeSnapshots[n.Name()]
			if !ok {
				return fmt.Errorf("task pipeline changed not using snapshot")
			}
			return nil
		})
		validSnapshot = err == nil
	}

	err := et.walk(func(n Node) error {
		if validSnapshot {
			n.start(snapshot.NodeSnapshots[n.Name()])
		} else {
			n.start(nil)
		}
		return nil
	})
	if err != nil {
		return err
	}
	et.stopping = make(chan struct{})
	if et.Task.SnapshotInterval > 0 {
		et.wg.Add(1)
		go et.runSnapshotter()
	}
	// Start calcThroughput
	et.wg.Add(1)
	go et.calcThroughput()
	return nil
}

func (et *ExecutingTask) stop() (err error) {
	close(et.stopping)
	_ = et.walk(func(n Node) error {
		n.stop()
		e := n.Wait()
		if e != nil {
			err = e
		}
		return nil
	})
	et.wg.Wait()
	return
}

var ErrWrongTaskType = errors.New("wrong task type")

// Instruct source batch node to start querying and sending batches of data
func (et *ExecutingTask) StartBatching() error {
	if et.Task.Type != BatchTask {
		return ErrWrongTaskType
	}

	batcher := et.source.(*BatchNode)

	err := et.checkDBRPs(batcher)
	if err != nil {
		batcher.Abort()
		return err
	}

	batcher.Start()
	return nil
}

func (et *ExecutingTask) BatchCount() (int, error) {
	if et.Task.Type != BatchTask {
		return 0, ErrWrongTaskType
	}

	batcher := et.source.(*BatchNode)
	return batcher.Count(), nil
}

// Get the next `num` batch queries that the batcher will run starting at time `start`.
func (et *ExecutingTask) BatchQueries(start, stop time.Time) ([]BatchQueries, error) {
	if et.Task.Type != BatchTask {
		return nil, ErrWrongTaskType
	}

	batcher := et.source.(*BatchNode)

	err := et.checkDBRPs(batcher)
	if err != nil {
		return nil, err
	}
	return batcher.Queries(start, stop)
}

// Check that the task allows access to DBRPs
func (et *ExecutingTask) checkDBRPs(batcher *BatchNode) error {
	dbMap := CreateDBRPMap(et.Task.DBRPs)
	dbrps, err := batcher.DBRPs()
	if err != nil {
		return err
	}
	for _, dbrp := range dbrps {
		if !dbMap[dbrp] {
			return fmt.Errorf("batch query is not allowed to request data from %v", dbrp)
		}
	}
	return nil
}

// Stop all stats nodes
func (et *ExecutingTask) StopStats() {
	_ = et.walk(func(n Node) error {
		if s, ok := n.(*StatsNode); ok {
			s.stopStats()
		}
		return nil
	})
}

// Wait till the task finishes and return any error
func (et *ExecutingTask) Wait() error {
	return et.rwalk(func(n Node) error {
		return n.Wait()
	})
}

// Get a named output.
func (et *ExecutingTask) GetOutput(name string) (Output, error) {
	if o, ok := et.outputs[name]; ok {
		return o, nil
	} else {
		return nil, fmt.Errorf("unknown output %s", name)
	}
}

// Register a named output.
func (et *ExecutingTask) registerOutput(name string, o Output) {
	et.outputs[name] = o
}

type ExecutionStats struct {
	TaskStats map[string]interface{}
	NodeStats map[string]map[string]interface{}
}

func (et *ExecutingTask) ExecutionStats() (ExecutionStats, error) {
	executionStats := ExecutionStats{
		TaskStats: make(map[string]interface{}),
		NodeStats: make(map[string]map[string]interface{}),
	}

	// Fill the task stats
	executionStats.TaskStats["throughput"] = et.getThroughput()

	// Fill the nodes stats
	err := et.walk(func(node Node) error {
		nodeStats := node.stats()

		// Add collected and emitted
		nodeStats["collected"] = node.collectedCount()
		nodeStats["emitted"] = node.emittedCount()

		executionStats.NodeStats[node.Name()] = nodeStats

		return nil
	})

	if err != nil {
		return executionStats, err
	}

	return executionStats, nil
}

// Return a graphviz .dot formatted byte array.
// Label edges with relavant execution information.
func (et *ExecutingTask) EDot(labels bool) []byte {

	var buf bytes.Buffer

	buf.Write([]byte("digraph "))
	buf.Write([]byte(et.Task.ID))
	buf.Write([]byte(" {\n"))
	// Write graph attributes
	unit := "points"
	if et.Task.Type == BatchTask {
		unit = "batches"
	}
	if labels {
		buf.Write([]byte(
			fmt.Sprintf("graph [label=\"Throughput: %0.2f %s/s\"];\n",
				et.getThroughput(),
				unit,
			),
		))
	} else {
		buf.Write([]byte(
			fmt.Sprintf("graph [throughput=\"%0.2f %s/s\"];\n",
				et.getThroughput(),
				unit,
			),
		))
	}

	_ = et.walk(func(n Node) error {
		n.edot(&buf, labels)
		return nil
	})
	buf.Write([]byte("}"))

	return buf.Bytes()
}

// Return the current throughput value.
func (et *ExecutingTask) getThroughput() float64 {
	et.tmu.RLock()
	defer et.tmu.RUnlock()
	return et.throughput
}

func (et *ExecutingTask) calcThroughput() {
	defer et.wg.Done()
	var previous int64
	last := time.Now()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			current := et.source.collectedCount()
			now := time.Now()
			elapsed := float64(now.Sub(last)) / float64(time.Second)

			et.tmu.Lock()
			et.throughput = float64(current-previous) / elapsed
			et.tmu.Unlock()

			last = now
			previous = current

		case <-et.stopping:
			return
		}
	}
}

// Create a  node from a given pipeline node.
func (et *ExecutingTask) createNode(p pipeline.Node, l *log.Logger) (n Node, err error) {
	switch t := p.(type) {
	case *pipeline.FromNode:
		n, err = newFromNode(et, t, l)
	case *pipeline.StreamNode:
		n, err = newStreamNode(et, t, l)
	case *pipeline.BatchNode:
		n, err = newBatchNode(et, t, l)
	case *pipeline.QueryNode:
		n, err = newQueryNode(et, t, l)
	case *pipeline.WindowNode:
		n, err = newWindowNode(et, t, l)
	case *pipeline.HTTPOutNode:
		n, err = newHTTPOutNode(et, t, l)
	case *pipeline.InfluxDBOutNode:
		n, err = newInfluxDBOutNode(et, t, l)
	case *pipeline.AlertNode:
		n, err = newAlertNode(et, t, l)
	case *pipeline.GroupByNode:
		n, err = newGroupByNode(et, t, l)
	case *pipeline.UnionNode:
		n, err = newUnionNode(et, t, l)
	case *pipeline.JoinNode:
		n, err = newJoinNode(et, t, l)
	case *pipeline.FlattenNode:
		n, err = newFlattenNode(et, t, l)
	case *pipeline.EvalNode:
		n, err = newEvalNode(et, t, l)
	case *pipeline.WhereNode:
		n, err = newWhereNode(et, t, l)
	case *pipeline.SampleNode:
		n, err = newSampleNode(et, t, l)
	case *pipeline.DerivativeNode:
		n, err = newDerivativeNode(et, t, l)
	case *pipeline.UDFNode:
		n, err = newUDFNode(et, t, l)
	case *pipeline.StatsNode:
		n, err = newStatsNode(et, t, l)
	case *pipeline.ShiftNode:
		n, err = newShiftNode(et, t, l)
	case *pipeline.NoOpNode:
		n, err = newNoOpNode(et, t, l)
	case *pipeline.InfluxQLNode:
		n, err = newInfluxQLNode(et, t, l)
	case *pipeline.LogNode:
		n, err = newLogNode(et, t, l)
	case *pipeline.DefaultNode:
		n, err = newDefaultNode(et, t, l)
	case *pipeline.DeleteNode:
		n, err = newDeleteNode(et, t, l)
	case *pipeline.CombineNode:
		n, err = newCombineNode(et, t, l)
	case *pipeline.K8sAutoscaleNode:
		n, err = newK8sAutoscaleNode(et, t, l)
	default:
		return nil, fmt.Errorf("unknown pipeline node type %T", p)
	}
	if err == nil && n != nil {
		n.init()
	}
	return n, err
}

type TaskSnapshot struct {
	NodeSnapshots map[string][]byte
}

func (et *ExecutingTask) Snapshot() (*TaskSnapshot, error) {
	snapshot := &TaskSnapshot{
		NodeSnapshots: make(map[string][]byte),
	}
	err := et.walk(func(n Node) error {
		data, err := n.snapshot()
		if err != nil {
			return err
		}
		snapshot.NodeSnapshots[n.Name()] = data
		return nil
	})
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

func (et *ExecutingTask) runSnapshotter() {
	defer et.wg.Done()
	// Wait random duration to splay snapshot events across interval
	select {
	case <-time.After(time.Duration(rand.Float64() * float64(et.Task.SnapshotInterval))):
	case <-et.stopping:
		return
	}
	ticker := time.NewTicker(et.Task.SnapshotInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			snapshot, err := et.Snapshot()
			if err != nil {
				et.logger.Println("E! failed to snapshot task", et.Task.ID, err)
				break
			}
			size := 0
			for _, data := range snapshot.NodeSnapshots {
				size += len(data)
			}
			// Only save the snapshot if it has content
			if size > 0 {
				err = et.tm.TaskStore.SaveSnapshot(et.Task.ID, snapshot)
				if err != nil {
					et.logger.Println("E! failed to save task snapshot", et.Task.ID, err)
				}
			}
		case <-et.stopping:
			return
		}
	}
}
