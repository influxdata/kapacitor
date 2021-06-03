package kapacitor

import (
	"bytes"
	"expvar"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/kapacitor/alert"
	"github.com/influxdata/kapacitor/edge"
	kexpvar "github.com/influxdata/kapacitor/expvar"
	"github.com/influxdata/kapacitor/keyvalue"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/server/vars"
	"github.com/influxdata/kapacitor/timer"
	"github.com/pkg/errors"
)

const (
	statErrorCount       = "errors"
	statCardinalityGauge = "working_cardinality"
	statAverageExecTime  = "avg_exec_time_ns"
)

type NodeDiagnostic interface {
	Error(msg string, err error, ctx ...keyvalue.T)

	// AlertNode
	AlertTriggered(level alert.Level, id string, message string, rows *models.Row)

	// AutoscaleNode
	SettingReplicas(new int, old int, id string)

	// QueryNode
	StartingBatchQuery(q string)

	// LogNode
	LogPointData(key, prefix string, data edge.PointMessage)
	LogBatchData(key, prefix string, data edge.BufferedBatchMessage)

	//UDF
	UDFLog(s string)
}

type nodeDiagnostic struct {
	NodeDiagnostic
	node *node
}

func newNodeDiagnostic(n *node, diag NodeDiagnostic) *nodeDiagnostic {
	return &nodeDiagnostic{
		NodeDiagnostic: diag,
		node:           n,
	}
}

func (n *nodeDiagnostic) Error(msg string, err error, ctx ...keyvalue.T) {
	n.node.incrementErrorCount()
	if !n.node.quiet {
		n.NodeDiagnostic.Error(msg, err, ctx...)
	}
}

// A node that can be  in an executor.
type Node interface {
	pipeline.Node

	addParentEdge(edge.StatsEdge)

	init(quiet bool)

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

	incrementErrorCount()

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
	ins        []edge.StatsEdge
	outs       []edge.StatsEdge
	diag       NodeDiagnostic
	timer      timer.Timer
	statsKey   string
	statMap    *kexpvar.Map

	quiet bool

	nodeErrors *kexpvar.Int
}

func (n *node) addParentEdge(e edge.StatsEdge) {
	n.ins = append(n.ins, e)
}

func (n *node) abortParentEdges() {
	for _, in := range n.ins {
		in.Abort()
	}
}

func (n *node) init(quiet bool) {
	tags := map[string]string{
		"task": n.et.Task.ID,
		"node": n.Name(),
		"type": n.et.Task.Type.String(),
		"kind": n.Desc(),
	}
	n.statsKey, n.statMap = vars.NewStatistic("nodes", tags)
	avgExecVar := &MaxDuration{}
	n.statMap.Set(statAverageExecTime, avgExecVar)
	n.nodeErrors = &kexpvar.Int{}
	n.statMap.Set(statErrorCount, n.nodeErrors)
	n.diag = newNodeDiagnostic(n, n.diag)
	n.statMap.Set(statCardinalityGauge, kexpvar.NewIntFuncGauge(nil))
	n.timer = n.et.tm.TimingService.NewTimer(avgExecVar)
	n.errCh = make(chan error, 1)
	n.quiet = quiet
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
				n.diag.Error("node failed", err)

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
	vars.DeleteStatistic(n.statsKey)
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

func (n *node) addChild(c Node) (edge.StatsEdge, error) {
	if n.Provides() != c.Wants() {
		return nil, fmt.Errorf("cannot add child mismatched edges: %s:%s -> %s:%s", n.Name(), n.Provides(), c.Name(), c.Wants())
	}
	if n.Provides() == pipeline.NoEdge {
		return nil, fmt.Errorf("cannot add child no edge expected: %s:%s -> %s:%s", n.Name(), n.Provides(), c.Name(), c.Wants())
	}
	n.children = append(n.children, c)

	d := n.et.tm.diag.WithEdgeContext(n.et.Task.ID, n.Name(), c.Name())
	edge := newEdge(n.et.Task.ID, n.Name(), c.Name(), n.Provides(), defaultEdgeBufferSize, d)
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
		buf.WriteString(
			fmt.Sprintf("\n%s [xlabel=\"",
				n.Name(),
			),
		)
		i := 0
		n.statMap.DoSorted(func(kv expvar.KeyValue) {
			if i != 0 {
				// NOTE: A literal \r, indicates a newline right justified in graphviz syntax.
				buf.WriteString(`\r`)
			}
			i++
			var s string
			if sv, ok := kv.Value.(kexpvar.StringVar); ok {
				s = sv.StringValue()
			} else {
				s = kv.Value.String()
			}
			buf.WriteString(
				fmt.Sprintf("%s=%s",
					kv.Key,
					s,
				),
			)
		})
		buf.Write([]byte("\"];\n"))

		for i, c := range n.children {
			buf.Write([]byte(
				fmt.Sprintf("%s -> %s [label=\"processed=%d\"];\n",
					n.Name(),
					c.Name(),
					n.outs[i].Collected(),
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
					n.outs[i].Collected(),
				),
			))
		}
	}
}

// node collected count is the sum of emitted counts of parent edges
func (n *node) collectedCount() (count int64) {
	for _, in := range n.ins {
		count += in.Emitted()
	}
	return
}

// node emitted count is the sum of collected counts of children edges
func (n *node) emittedCount() (count int64) {
	for _, out := range n.outs {
		count += out.Collected()
	}
	return
}

// node increment error count increments a nodes error_count stat
func (n *node) incrementErrorCount() {
	n.nodeErrors.Add(1)
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
	Dimensions models.Dimensions
}

// Return a copy of the current node statistics.
// If if no groups have been seen yet a NilGroup will be created with zero stats.
func (n *node) nodeStatsByGroup() (stats map[models.GroupID]nodeStats) {
	// Get the counts for just one output.
	stats = make(map[models.GroupID]nodeStats)
	if len(n.outs) > 0 {
		n.outs[0].ReadGroupStats(func(g *edge.GroupStats) {
			stats[g.GroupInfo.ID] = nodeStats{
				Fields: models.Fields{
					// A node's emitted count is the collected count of its output.
					"emitted": g.Collected,
				},
				Tags:       g.GroupInfo.Tags,
				Dimensions: g.GroupInfo.Dimensions,
			}
		})
	}
	if len(stats) == 0 {
		// If we have no groups/stats add nil group with emitted = 0
		stats[""] = nodeStats{
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
