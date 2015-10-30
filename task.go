package kapacitor

import (
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/influxdb/kapacitor/pipeline"
	"github.com/influxdb/kapacitor/tick"
	"github.com/influxdb/kapacitor/wlog"
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

type DBRP struct {
	Database        string
	RetentionPolicy string
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

// The complete definition of a task, its name, pipeline and type.
type Task struct {
	Name     string
	Pipeline *pipeline.Pipeline
	Type     TaskType
	DBRPs    []DBRP
}

func NewTask(name, script string, tt TaskType, dbrps []DBRP) (*Task, error) {
	t := &Task{
		Name:  name,
		Type:  tt,
		DBRPs: dbrps,
	}

	var srcEdge pipeline.EdgeType
	switch tt {
	case StreamTask:
		srcEdge = pipeline.StreamEdge
	case BatchTask:
		srcEdge = pipeline.BatchEdge
	}

	scope := tick.NewScope()
	scope.Set("influxql", newInfluxQL())
	scope.Set("time", func(d time.Duration) time.Duration { return d })

	p, err := pipeline.CreatePipeline(script, srcEdge, scope)
	if err != nil {
		return nil, err
	}
	t.Pipeline = p
	return t, nil
}

func (t *Task) Dot() []byte {
	return t.Pipeline.Dot(t.Name)
}

// Create a new streamer task from a script.
func NewStreamer(name, script string, dbrps []DBRP) (*Task, error) {
	return NewTask(name, script, StreamTask, dbrps)
}

// Create a new batcher task from a script.
func NewBatcher(name, script string, dbrps []DBRP) (*Task, error) {
	return NewTask(name, script, BatchTask, dbrps)
}

// ----------------------------------
// ExecutingTask

// A task that is ready for execution.
type ExecutingTask struct {
	tm      *TaskMaster
	Task    *Task
	ins     []*Edge
	source  Node
	outputs map[string]Output
	// node lookup from pipeline.ID -> Node
	lookup map[pipeline.ID]Node
	nodes  []Node
}

// Create a new  task from a defined kapacitor.
func NewExecutingTask(tm *TaskMaster, t *Task) (*ExecutingTask, error) {
	et := &ExecutingTask{
		tm:      tm,
		Task:    t,
		outputs: make(map[string]Output),
		lookup:  make(map[pipeline.ID]Node),
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
		en, err := et.createNode(n)
		if err != nil {
			return err
		}
		en.setLogger(wlog.New(
			os.Stderr,
			fmt.Sprintf("[%s:%s] ", et.Task.Name, en.Name()),
			log.LstdFlags,
		))
		et.lookup[n.ID()] = en
		// Save the walk order
		et.nodes = append(et.nodes, en)
		// Set source node to task name
		if len(et.nodes) == 1 {
			et.nodes[0].SetName(et.Task.Name)
		}
		// Duplicate the Edges
		for _, p := range n.Parents() {
			ep := et.lookup[p.ID()]
			ep.linkChild(en)
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
func (et *ExecutingTask) start(ins []*Edge) error {
	et.ins = ins

	for _, in := range et.ins {
		et.source.addParentEdge(in)
	}

	return et.walk(func(n Node) error {
		n.start()
		return nil
	})
}

func (et *ExecutingTask) stop() {
	et.rwalk(func(n Node) error {
		n.stop()
		return nil
	})
	for _, in := range et.ins {
		in.Close()
	}
}

var ErrWrongTaskType = errors.New("wrong task type")

// Instruct source batch node to start querying and sending batches of data
func (et *ExecutingTask) StartBatching() error {
	if et.Task.Type != BatchTask {
		return ErrWrongTaskType
	}

	batcher := et.source.(*SourceBatchNode)

	err := et.checkDBRPs(batcher)
	if err != nil {
		return err
	}

	batcher.Start(et.ins)
	return nil
}

func (et *ExecutingTask) BatchCount() (int, error) {
	if et.Task.Type != BatchTask {
		return 0, ErrWrongTaskType
	}

	batcher := et.source.(*SourceBatchNode)
	return batcher.Count(), nil
}

// Get the next `num` batch queries that the batcher will run starting at time `start`.
func (et *ExecutingTask) BatchQueries(start, stop time.Time) ([][]string, error) {
	if et.Task.Type != BatchTask {
		return nil, ErrWrongTaskType
	}

	batcher := et.source.(*SourceBatchNode)

	err := et.checkDBRPs(batcher)
	if err != nil {
		return nil, err
	}

	return batcher.Queries(start, stop), nil
}

// Check that the task allows access to DBRPs
func (et *ExecutingTask) checkDBRPs(batcher *SourceBatchNode) error {
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

// Wait till the task finishes and return any error
func (et *ExecutingTask) Err() error {
	return et.rwalk(func(n Node) error {
		return n.Err()
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

// Create a  node from a given pipeline node.
func (et *ExecutingTask) createNode(p pipeline.Node) (Node, error) {
	switch t := p.(type) {
	case *pipeline.StreamNode:
		return newStreamNode(et, t)
	case *pipeline.SourceBatchNode:
		return newSourceBatchNode(et, t)
	case *pipeline.BatchNode:
		return newBatchNode(et, t)
	case *pipeline.WindowNode:
		return newWindowNode(et, t)
	case *pipeline.HTTPOutNode:
		return newHTTPOutNode(et, t)
	case *pipeline.InfluxDBOutNode:
		return newInfluxDBOutNode(et, t)
	case *pipeline.MapNode:
		return newMapNode(et, t)
	case *pipeline.ReduceNode:
		return newReduceNode(et, t)
	case *pipeline.AlertNode:
		return newAlertNode(et, t)
	case *pipeline.GroupByNode:
		return newGroupByNode(et, t)
	case *pipeline.UnionNode:
		return newUnionNode(et, t)
	case *pipeline.JoinNode:
		return newJoinNode(et, t)
	case *pipeline.EvalNode:
		return newApplyNode(et, t)
	case *pipeline.WhereNode:
		return newWhereNode(et, t)
	default:
		return nil, fmt.Errorf("unknown pipeline node type %T", p)
	}
}
