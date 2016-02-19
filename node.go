package kapacitor

import (
	"bytes"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/influxdata/kapacitor/expvar"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/timer"
)

const (
	averageExecTimeVarName = "avg_execution_time"
)

// A node that can be  in an executor.
type Node interface {
	pipeline.Node

	addParentEdge(*Edge)

	// start the node and its children
	start(snapshot []byte)
	stop()

	// snapshot running state
	snapshot() ([]byte, error)
	restore(snapshot []byte) error

	// wait for the node to finish processing and return any errors
	Err() error

	// link specified child
	linkChild(c Node) error
	addParent(p Node)

	// close children edges
	closeChildEdges()
	// abort parent edges
	abortParentEdges()

	// executing dot
	edot(buf *bytes.Buffer)

	nodeStatsByGroup() map[models.GroupID]nodeStats

	collectedCount() int64
}

//implementation of Node
type node struct {
	pipeline.Node
	et         *ExecutingTask
	parents    []Node
	children   []Node
	runF       func(snapshot []byte) error
	stopF      func()
	errCh      chan error
	err        error
	finishedMu sync.Mutex
	finished   bool
	ins        []*Edge
	outs       []*Edge
	logger     *log.Logger
	timer      timer.Timer
	statsKey   string
	statMap    *expvar.Map
	avgExecVar *expvar.MaxFloat
}

func (n *node) addParentEdge(e *Edge) {
	n.ins = append(n.ins, e)
}

func (n *node) abortParentEdges() {
	for _, in := range n.ins {
		in.Abort()
	}
}

func (n *node) start(snapshot []byte) {
	tags := map[string]string{
		"task": n.et.Task.Name,
		"node": n.Name(),
		"type": n.et.Task.Type.String(),
		"kind": n.Desc(),
	}
	n.statsKey, n.statMap = NewStatistics("nodes", tags)
	n.avgExecVar = &expvar.MaxFloat{}
	n.statMap.Set(averageExecTimeVarName, n.avgExecVar)
	n.timer = n.et.tm.TimingService.NewTimer(n.avgExecVar)
	n.errCh = make(chan error, 1)
	go func() {
		var err error
		defer func() {
			// Always close children edges
			n.closeChildEdges()
			// Propogate error up
			if err != nil {
				// Handle panic in runF
				r := recover()
				if r != nil {
					trace := make([]byte, 512)
					n := runtime.Stack(trace, false)
					err = fmt.Errorf("%s: Trace:%s", r, string(trace[:n]))
				}
				n.abortParentEdges()
				n.logger.Println("E!", err)
			}
			n.errCh <- err
		}()
		// Run node
		err = n.runF(snapshot)
	}()
}

func (n *node) stop() {
	if n.stopF != nil {
		n.stopF()
	}
	DeleteStatistics(n.statsKey)
}

// no-op snapshot
func (n *node) snapshot() (b []byte, err error) { return }

// no-op restore
func (n *node) restore([]byte) error { return nil }

func (n *node) Err() error {
	n.finishedMu.Lock()
	defer n.finishedMu.Unlock()
	if !n.finished {
		n.finished = true
		n.err = <-n.errCh
	}
	return n.err
}

func (n *node) addChild(c Node) (*Edge, error) {
	if n.Provides() != c.Wants() {
		return nil, fmt.Errorf("cannot add child mismatched edges: %s -> %s", n.Provides(), c.Wants())
	}
	n.children = append(n.children, c)

	edge := newEdge(n.et.Task.Name, n.Name(), c.Name(), n.Provides(), n.et.tm.LogService)
	if edge == nil {
		return nil, fmt.Errorf("unknown edge type %s", n.Provides())
	}
	c.addParentEdge(edge)
	return edge, nil
}

func (n *node) addParent(p Node) {
	n.parents = append(n.parents, p)
}

func (n *node) linkChild(c Node) error {

	// add child
	edge, err := n.addChild(c)
	if err != nil {
		return err
	}

	// add parent
	c.addParent(n)

	// store edge to child
	n.outs = append(n.outs, edge)
	return nil
}

func (n *node) closeChildEdges() {
	for _, child := range n.outs {
		child.Close()
	}
}

func (n *node) edot(buf *bytes.Buffer) {
	buf.Write([]byte(
		fmt.Sprintf("\n%s [label=\"%s %v\"];\n",
			n.Name(),
			n.Name(),
			time.Duration(n.avgExecVar.Get()),
		),
	))
	for i, c := range n.children {
		buf.Write([]byte(
			fmt.Sprintf("%s -> %s [label=\"%d\"];\n",
				n.Name(),
				c.Name(),
				n.outs[i].collectedCount(),
			),
		))
	}
}

func (n *node) collectedCount() (count int64) {
	for _, in := range n.ins {
		count += in.emittedCount()
	}
	return
}

// Statistics for a node
type nodeStats struct {
	Fields     models.Fields
	Tags       models.Tags
	Dimensions []string
}

// Return a copy of the current node statistics.
func (n *node) nodeStatsByGroup() (stats map[models.GroupID]nodeStats) {
	// Get the counts for just one output.
	if len(n.outs) > 0 {
		stats = make(map[models.GroupID]nodeStats)
		n.outs[0].readGroupStats(func(group models.GroupID, c, e int64, tags models.Tags, dims []string) {
			stats[group] = nodeStats{
				Fields: models.Fields{
					// A node's emitted count is the collected count of its output.
					"emitted": c,
				},
				Tags:       tags,
				Dimensions: dims,
			}
		})
	}
	return
}
