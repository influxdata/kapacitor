package kapacitor

import (
	"bytes"
	"expvar"
	"fmt"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	kexpvar "github.com/influxdata/kapacitor/expvar"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/timer"
	"github.com/pkg/errors"
)

const (
	statAverageExecTime = "avg_exec_time_ns"
)

// A node that can be  in an executor.
type Node interface {
	pipeline.Node

	addParentEdge(*Edge)

	init()

	// start the node and its children
	start(snapshot []byte)
	stop()

	// snapshot running state
	snapshot() ([]byte, error)
	restore(snapshot []byte) error

	// wait for the node to finish processing and return any errors
	Wait() error

	// link specified child
	linkChild(c Node) error
	addParent(p Node)

	// close children edges
	closeChildEdges()
	// abort parent edges
	abortParentEdges()

	// executing dot
	edot(buf *bytes.Buffer, labels bool)

	nodeStatsByGroup() map[models.GroupID]nodeStats

	collectedCount() int64

	emittedCount() int64

	stats() map[string]interface{}
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
	statMap    *kexpvar.Map
}

func (n *node) addParentEdge(e *Edge) {
	n.ins = append(n.ins, e)
}

func (n *node) abortParentEdges() {
	for _, in := range n.ins {
		in.Abort()
	}
}

func (n *node) init() {
	tags := map[string]string{
		"task": n.et.Task.ID,
		"node": n.Name(),
		"type": n.et.Task.Type.String(),
		"kind": n.Desc(),
	}
	n.statsKey, n.statMap = NewStatistics("nodes", tags)
	avgExecVar := &MaxDuration{}
	n.statMap.Set(statAverageExecTime, avgExecVar)
	n.timer = n.et.tm.TimingService.NewTimer(avgExecVar)
	n.errCh = make(chan error, 1)
}

func (n *node) start(snapshot []byte) {
	go func() {
		var err error
		defer func() {
			// Always close children edges
			n.closeChildEdges()
			// Propagate error up
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
				err = errors.Wrap(err, n.Name())
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

func (n *node) Wait() error {
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
		return nil, fmt.Errorf("cannot add child mismatched edges: %s:%s -> %s:%s", n.Name(), n.Provides(), c.Name(), c.Wants())
	}
	if n.Provides() == pipeline.NoEdge {
		return nil, fmt.Errorf("cannot add child no edge expected: %s:%s -> %s:%s", n.Name(), n.Provides(), c.Name(), c.Wants())
	}
	n.children = append(n.children, c)

	edge := newEdge(n.et.Task.ID, n.Name(), c.Name(), n.Provides(), defaultEdgeBufferSize, n.et.tm.LogService)
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

func (n *node) edot(buf *bytes.Buffer, labels bool) {
	if labels {
		// Print all stats on node.
		buf.Write([]byte(
			fmt.Sprintf("\n%s [label=\"%s ",
				n.Name(),
				n.Name(),
			),
		))
		n.statMap.DoSorted(func(kv expvar.KeyValue) {
			buf.Write([]byte(
				fmt.Sprintf("%s=%s ",
					kv.Key,
					kv.Value.String(),
				),
			))
		})
		buf.Write([]byte("\"];\n"))

		for i, c := range n.children {
			buf.Write([]byte(
				fmt.Sprintf("%s -> %s [label=\"%d\"];\n",
					n.Name(),
					c.Name(),
					n.outs[i].collectedCount(),
				),
			))
		}

	} else {
		// Print all stats on node.
		buf.Write([]byte(
			fmt.Sprintf("\n%s [",
				n.Name(),
			),
		))
		n.statMap.DoSorted(func(kv expvar.KeyValue) {
			var s string
			if sv, ok := kv.Value.(kexpvar.StringVar); ok {
				s = sv.StringValue()
			} else {
				s = kv.Value.String()
			}
			buf.Write([]byte(
				fmt.Sprintf("%s=\"%s\" ",
					kv.Key,
					s,
				),
			))
		})
		buf.Write([]byte("];\n"))
		for i, c := range n.children {
			buf.Write([]byte(
				fmt.Sprintf("%s -> %s [processed=\"%d\"];\n",
					n.Name(),
					c.Name(),
					n.outs[i].collectedCount(),
				),
			))
		}
	}
}

// node collected count is the sum of emitted counts of parent edges
func (n *node) collectedCount() (count int64) {
	for _, in := range n.ins {
		count += in.emittedCount()
	}
	return
}

// node emitted count is the sum of collected counts of children edges
func (n *node) emittedCount() (count int64) {
	for _, out := range n.outs {
		count += out.collectedCount()
	}
	return
}

func (n *node) stats() map[string]interface{} {
	stats := make(map[string]interface{})

	n.statMap.Do(func(kv expvar.KeyValue) {
		switch v := kv.Value.(type) {
		case kexpvar.IntVar:
			stats[kv.Key] = v.IntValue()
		case kexpvar.FloatVar:
			stats[kv.Key] = v.FloatValue()
		default:
			stats[kv.Key] = v.String()
		}
	})

	return stats
}

// Statistics for a node
type nodeStats struct {
	Fields     models.Fields
	Tags       models.Tags
	Dimensions []string
}

// Return a copy of the current node statistics.
// If if no groups have been seen yet a NilGroup will be created with zero stats.
func (n *node) nodeStatsByGroup() (stats map[models.GroupID]nodeStats) {
	// Get the counts for just one output.
	stats = make(map[models.GroupID]nodeStats)
	if len(n.outs) > 0 {
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
	if len(stats) == 0 {
		// If we have no groups/stats add nil group with emitted = 0
		stats[models.NilGroup] = nodeStats{
			Fields: models.Fields{
				"emitted": int64(0),
			},
		}
	}
	return
}

// MaxDuration is a 64-bit int variable representing a duration in nanoseconds,that satisfies the expvar.Var interface.
// When setting a value it will only be set if it is greater than the current value.
type MaxDuration struct {
	d      int64
	setter timer.Setter
}

func (v *MaxDuration) String() string {
	return `"` + v.StringValue() + `"`
}

func (v *MaxDuration) StringValue() string {
	return time.Duration(v.IntValue()).String()
}

func (v *MaxDuration) IntValue() int64 {
	return atomic.LoadInt64(&v.d)
}

// Set sets value if it is greater than current value.
// If set was successful and a setter exists, will pass on value to setter.
func (v *MaxDuration) Set(next int64) {
	for {
		cur := v.IntValue()
		if next > cur {
			if atomic.CompareAndSwapInt64(&v.d, cur, next) {
				if v.setter != nil {
					v.setter.Set(next)
				}
				return
			}
		} else {
			return
		}
	}
}
