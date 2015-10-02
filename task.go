package kapacitor

import (
	"fmt"
	"time"

	"github.com/influxdb/kapacitor/pipeline"
	"github.com/influxdb/kapacitor/tick"
)

// The type of a task
type TaskType int

const (
	StreamerTask TaskType = iota
	BatcherTask
)

func (t TaskType) String() string {
	switch t {
	case StreamerTask:
		return "streamer"
	case BatcherTask:
		return "batcher"
	default:
		return "unknown"
	}
}

// The complete definition of a task, its name, pipeline and type.
type Task struct {
	Name     string
	Pipeline *pipeline.Pipeline
	Type     TaskType
}

func NewTask(name, script string, tt TaskType) (*Task, error) {
	t := &Task{
		Name: name,
		Type: tt,
	}

	var srcEdge pipeline.EdgeType
	switch tt {
	case StreamerTask:
		srcEdge = pipeline.StreamEdge
	case BatcherTask:
		srcEdge = pipeline.BatchEdge
	}

	scope := tick.NewScope()
	scope.Set("influxql", newInfluxQL())
	scope.Set("expr", ExprFunc)
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

func (t *Task) Query() (*Query, error) {
	if t.Type == BatcherTask {
		bn := t.Pipeline.Source.(*pipeline.BatchNode)
		query := bn.Query
		q, err := NewQuery(query)
		if err != nil {
			return nil, err
		}
		q.Dimensions(bn.Dimensions)
		return q, nil
	}
	return nil, fmt.Errorf("not a batcher, does not have query")
}

func (t *Task) Period() time.Duration {
	if t.Type == BatcherTask {
		return t.Pipeline.Source.(*pipeline.BatchNode).Period
	}
	return 0
}

// Create a new streamer task from a script.
func NewStreamer(name, script string) (*Task, error) {
	return NewTask(name, script, StreamerTask)
}

// Create a new batcher task from a script.
func NewBatcher(name, script string) (*Task, error) {
	return NewTask(name, script, BatcherTask)
}

// ----------------------------------
// ExecutingTask

// A task that can be  in an executor.
type ExecutingTask struct {
	tm      *TaskMaster
	Task    *Task
	in      *Edge
	source  Node
	outputs map[string]Output
	// node lookup from pipeline.ID -> Node
	lookup map[pipeline.ID]Node
	nodes  []Node
}

// Create a new  task from a defined kapacitor.
func NewExecutingTask(tm *TaskMaster, t *Task) *ExecutingTask {
	et := &ExecutingTask{
		tm:      tm,
		Task:    t,
		outputs: make(map[string]Output),
		lookup:  make(map[pipeline.ID]Node),
	}
	return et
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
		et.lookup[n.ID()] = en
		// Save the walk order
		et.nodes = append(et.nodes, en)
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
func (et *ExecutingTask) start(in *Edge) error {
	et.in = in
	err := et.link()
	if err != nil {
		return err
	}

	et.source.addParentEdge(et.in)

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
	et.in.Close()
}

// Instruct source batch node to start querying and sending batches of data
func (et *ExecutingTask) StartBatching() {
	if et.Task.Type != BatcherTask {
		return
	}

	batcher := et.source.(*BatchNode)
	go batcher.Query(et.in)
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
	case *pipeline.BatchNode:
		return newBatchNode(et, t)
	case *pipeline.WindowNode:
		return newWindowNode(et, t)
	case *pipeline.HTTPOutNode:
		return newHTTPOutNode(et, t)
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
	case *pipeline.ApplyNode:
		return newApplyNode(et, t)
	default:
		return nil, fmt.Errorf("unknown pipeline node type %T", p)
	}
}
