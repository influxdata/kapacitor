package pipeline

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	"github.com/influxdata/kapacitor/tick/ast"
)

// The type of data that travels along an edge connecting two nodes in a Pipeline.
type EdgeType int

const (
	// No data are transferred.
	NoEdge EdgeType = iota
	// Data are transferred immediately and one point at a time.
	StreamEdge
	// Data are transferred in batches as soon as the data are ready.
	BatchEdge
)

type ID int

func (e EdgeType) String() string {
	switch e {
	case NoEdge:
		return "noedge"
	case StreamEdge:
		return "stream"
	case BatchEdge:
		return "batch"
	default:
		return "unknown EdgeType"
	}
}

//Generic node in a pipeline
type Node interface {
	// List of parents of this node.
	Parents() []Node
	// List of children of this node.
	Children() []Node
	// Add a parent node only, does not add the child relation.
	addParent(p Node)
	// Links a child node by adding both the parent and child relation.
	linkChild(c Node)

	// Short description of the node does not need to be unique
	Desc() string

	// Friendly readable unique name of the node
	Name() string
	SetName(string)

	// Unique id for the node
	ID() ID
	setID(ID)

	// The type of input the node wants.
	Wants() EdgeType
	// The type of output the node provides.
	Provides() EdgeType

	// IsQuiet reports whether the node should suppress all errors during evaluation.
	IsQuiet() bool

	// Check that the definition of the node is consistent
	validate() error

	// Helper methods for walking DAG
	tMark() bool
	setTMark(b bool)
	pMark() bool
	setPMark(b bool)
	setPipeline(*Pipeline)
	pipeline() *Pipeline

	// Return .dot string to graph DAG
	dot(buf *bytes.Buffer)
}

type node struct {
	p        *Pipeline
	desc     string
	name     string
	id       ID
	parents  []Node
	children []Node
	wants    EdgeType
	provides EdgeType
	tm       bool
	pm       bool

	// tick:ignore
	QuietFlag bool `tick:"Quiet" json:"quiet,omitempty"`
}

// tick:ignore
func (n *node) IsQuiet() bool {
	return n.QuietFlag
}

// Quiet suppresses all error logging events from this node.
// tick:property
func (n *node) Quiet() {
	n.QuietFlag = true
}

// tick:ignore
func (n *node) Desc() string {
	return n.desc
}

// tick:ignore
func (n *node) ID() ID {
	return n.id
}

func (n *node) setID(id ID) {
	n.id = id
}

// tick:ignore
func (n *node) Name() string {
	if n.name == "" {
		n.name = fmt.Sprintf("%s%d", n.Desc(), n.ID())
	}
	return n.name
}

// tick:ignore
func (n *node) SetName(name string) {
	n.name = name
}

// tick:ignore
func (n *node) Parents() []Node {
	return n.parents
}

// tick:ignore
func (n *node) Children() []Node {
	return n.children
}

func (n *node) addParent(c Node) {
	n.parents = append(n.parents, c)
}

func (n *node) linkChild(c Node) {
	c.setPipeline(n.p)
	_ = n.p.assignID(c)
	n.children = append(n.children, c)
	c.addParent(n)
}

func (n *node) tMark() bool {
	return n.tm
}

func (n *node) setTMark(b bool) {
	n.tm = b
}

func (n *node) pMark() bool {
	return n.pm
}

func (n *node) setPMark(b bool) {
	n.pm = b
}

func (n *node) setPipeline(p *Pipeline) {
	n.p = p
}
func (n *node) pipeline() *Pipeline {
	return n.p
}

// tick:ignore
func (n *node) Wants() EdgeType {
	return n.wants
}

// tick:ignore
func (n *node) Provides() EdgeType {
	return n.provides
}

func (n *node) validate() error {
	return nil
}

func (n *node) dot(buf *bytes.Buffer) {
	for _, c := range n.children {
		buf.Write([]byte(fmt.Sprintf("%s -> %s;\n", n.Name(), c.Name())))
	}
}

// Create a new stream of data that contains the internal statistics of the node.
// The interval represents how often to emit the statistics based on real time.
// This means the interval time is independent of the times of the data points the source node is receiving.
func (n *node) Stats(interval time.Duration) *StatsNode {
	stats := newStatsNode(n, interval)
	n.pipeline().addSource(stats)
	// If the source node does not have any children add a NoOpNode.
	// This is a work around to make it so that the source node has somewhere to send its data.
	// That way we can get stats on its behavior.
	if len(n.Children()) == 0 {
		noop := newNoOpNode(n.Provides())
		n.linkChild(noop)
	}
	return stats
}

const nodeNameMarker = "NODE_NAME"
const intervalMarker = "INTERVAL"

// Helper function for creating an alert on low throughput, a.k.a. deadman's switch.
//
// - Threshold -- trigger alert if throughput drops below threshold in points/interval.
// - Interval -- how often to check the throughput.
// - Expressions -- optional list of expressions to also evaluate. Useful for time of day alerting.
//
// Example:
//    var data = stream
//        |from()...
//    // Trigger critical alert if the throughput drops below 100 points per 10s and checked every 10s.
//    data
//        |deadman(100.0, 10s)
//    //Do normal processing of data
//    data...
//
// The above is equivalent to this
// Example:
//    var data = stream
//        |from()...
//    // Trigger critical alert if the throughput drops below 100 points per 10s and checked every 10s.
//    data
//        |stats(10s)
//            .align()
//        |derivative('emitted')
//            .unit(10s)
//            .nonNegative()
//        |alert()
//            .id('node \'stream0\' in task \'{{ .TaskName }}\'')
//            .message('{{ .ID }} is {{ if eq .Level "OK" }}alive{{ else }}dead{{ end }}: {{ index .Fields "emitted" | printf "%0.3f" }} points/10s.')
//            .crit(lambda: "emitted" <= 100.0)
//    //Do normal processing of data
//    data...
//
// The `id` and `message` alert properties can be configured globally via the 'deadman' configuration section.
//
// Since the AlertNode is the last piece it can be further modified as usual.
// Example:
//    var data = stream
//        |from()...
//    // Trigger critical alert if the throughput drops below 100 points per 10s and checked every 10s.
//    data
//        |deadman(100.0, 10s)
//            .slack()
//            .channel('#dead_tasks')
//    //Do normal processing of data
//    data...
//
// You can specify additional lambda expressions to further constrain when the deadman's switch is triggered.
// Example:
//    var data = stream
//        |from()...
//    // Trigger critical alert if the throughput drops below 100 points per 10s and checked every 10s.
//    // Only trigger the alert if the time of day is between 8am-5pm.
//    data
//        |deadman(100.0, 10s, lambda: hour("time") >= 8 AND hour("time") <= 17)
//    //Do normal processing of data
//    data...
//
func (n *node) Deadman(threshold float64, interval time.Duration, expr ...*ast.LambdaNode) *AlertNode {
	dn := n.Stats(interval).Align().
		Derivative("emitted").NonNegative()
	dn.Unit = interval

	an := dn.Alert()
	critExpr := &ast.BinaryNode{
		Operator: ast.TokenLessEqual,
		Left: &ast.ReferenceNode{
			Reference: "emitted",
		},
		Right: &ast.NumberNode{
			IsFloat: true,
			Float64: threshold,
		},
	}
	// Add any additional expressions
	for _, e := range expr {
		critExpr = &ast.BinaryNode{
			Operator: ast.TokenAnd,
			Left:     critExpr,
			Right:    e.Expression,
		}
	}
	an.Crit = &ast.LambdaNode{Expression: critExpr}
	// Replace NODE_NAME with actual name of the node in the Id.
	an.Id = strings.Replace(n.pipeline().deadman.Id(), nodeNameMarker, n.Name(), 1)
	// Set the message on the alert node.
	an.Message = strings.Replace(n.pipeline().deadman.Message(), intervalMarker, interval.String(), 1)
	return an
}

// ---------------------------------
// Chaining methods
//

// basic implementation of node + chaining methods
type chainnode struct {
	node
}

func newBasicChainNode(desc string, wants, provides EdgeType) chainnode {
	return chainnode{node{
		desc:     desc,
		wants:    wants,
		provides: provides,
	}}
}

// Create a new node that filters the data stream by a given expression.
func (n *chainnode) Where(expression *ast.LambdaNode) *WhereNode {
	w := newWhereNode(n.provides, expression)
	n.linkChild(w)
	return w
}

// Create an HTTP output node that caches the most recent data it has received.
// The cached data are available at the given endpoint.
// The endpoint is the relative path from the API endpoint of the running task.
// For example, if the task endpoint is at `/kapacitor/v1/tasks/<task_id>` and endpoint is
// `top10`, then the data can be requested from `/kapacitor/v1/tasks/<task_id>/top10`.
func (n *chainnode) HttpOut(endpoint string) *HTTPOutNode {
	h := newHTTPOutNode(n.provides, endpoint)
	n.linkChild(h)
	return h
}

// Creates an HTTP Post node that POSTS received data to the provided HTTP endpoint.
// HttpPost expects 0 or 1 arguments. If 0 arguments are provided, you must specify an
// endpoint property method.
func (n *chainnode) HttpPost(url ...string) *HTTPPostNode {
	h := newHTTPPostNode(n.provides, url...)
	n.linkChild(h)
	return h
}

// Create an influxdb output node that will store the incoming data into InfluxDB.
func (n *chainnode) InfluxDBOut() *InfluxDBOutNode {
	i := newInfluxDBOutNode(n.provides)
	n.linkChild(i)
	return i
}

// Create an kapacitor loopback node that will send data back into Kapacitor as a stream.
func (n *chainnode) KapacitorLoopback() *KapacitorLoopbackNode {
	k := newKapacitorLoopbackNode(n.provides)
	n.linkChild(k)
	return k
}

// Create an alert node, which can trigger alerts.
func (n *chainnode) Alert() *AlertNode {
	a := newAlertNode(n.provides)
	n.linkChild(a)
	return a
}

// Perform the union of this node and all other given nodes.
func (n *chainnode) Union(node ...Node) *UnionNode {
	u := newUnionNode(n.provides, node)
	n.linkChild(u)
	return u
}

// Join this node with other nodes. The data are joined on timestamp.
func (n *chainnode) Join(others ...Node) *JoinNode {
	others = append([]Node{n}, others...)
	j := newJoinNode(n.provides, others)
	return j
}

// Combine this node with itself. The data are combined on timestamp.
func (n *chainnode) Combine(expressions ...*ast.LambdaNode) *CombineNode {
	c := newCombineNode(n.provides, expressions)
	n.linkChild(c)
	return c
}

// Flatten points with similar times into a single point.
func (n *chainnode) Flatten() *FlattenNode {
	f := newFlattenNode(n.provides)
	n.linkChild(f)
	return f
}

// Create an eval node that will evaluate the given transformation function to each data point.
// A list of expressions may be provided and will be evaluated in the order they are given.
// The results are available to later expressions.
func (n *chainnode) Eval(expressions ...*ast.LambdaNode) *EvalNode {
	e := newEvalNode(n.provides, expressions)
	n.linkChild(e)
	return e
}

// Group the data by a set of tags.
//
// Can pass literal * to group by all dimensions.
// Example:
//    |groupBy(*)
//
func (n *chainnode) GroupBy(tag ...interface{}) *GroupByNode {
	g := newGroupByNode(n.provides, tag)
	n.linkChild(g)
	return g
}

// Create a new node that windows the stream by time.
//
// NOTE: Window can only be applied to stream edges.
func (n *chainnode) Window() *WindowNode {
	if n.Provides() != StreamEdge {
		panic("cannot Window batch edge")
	}
	w := newWindowNode()
	n.linkChild(w)
	return w
}

// Create a new Barrier node that emits a BarrierMessage periodically
//
// One BarrierMessage will be emitted every period duration
func (n *chainnode) Barrier() *BarrierNode {
	b := newBarrierNode(n.provides)
	n.linkChild(b)
	return b
}

// Create a new node that samples the incoming points or batches.
//
// One point will be emitted every count or duration specified.
func (n *chainnode) Sample(rate interface{}) *SampleNode {
	s := newSampleNode(n.Provides(), rate)
	n.linkChild(s)
	return s
}

// Create a new node that computes the derivative of adjacent points.
func (n *chainnode) Derivative(field string) *DerivativeNode {
	s := newDerivativeNode(n.Provides(), field)
	n.linkChild(s)
	return s
}

// Create a new node that only emits new points if different from the previous point
func (n *chainnode) ChangeDetect(fields ...string) *ChangeDetectNode {
	s := newChangeDetectNode(n.Provides(), fields)
	n.linkChild(s)
	return s
}

// Create a new node that shifts the incoming points or batches in time.
func (n *chainnode) Shift(shift time.Duration) *ShiftNode {
	s := newShiftNode(n.Provides(), shift)
	n.linkChild(s)
	return s
}

// Create a node that logs all data it receives.
func (n *chainnode) Log() *LogNode {
	s := newLogNode(n.Provides())
	n.linkChild(s)
	return s
}

// Create a node that can set defaults for missing tags or fields.
func (n *chainnode) Default() *DefaultNode {
	s := newDefaultNode(n.Provides())
	n.linkChild(s)
	return s
}

// Create a node that can delete tags or fields.
func (n *chainnode) Delete() *DeleteNode {
	s := newDeleteNode(n.Provides())
	n.linkChild(s)
	return s
}

// Create a node that can trigger autoscale events for a kubernetes cluster.
func (n *chainnode) K8sAutoscale() *K8sAutoscaleNode {
	k := newK8sAutoscaleNode(n.Provides())
	n.linkChild(k)
	return k
}

// Create a node that can trigger autoscale events for a docker swarm cluster.
func (n *chainnode) SwarmAutoscale() *SwarmAutoscaleNode {
	k := newSwarmAutoscaleNode(n.Provides())
	n.linkChild(k)
	return k
}

// Create a node that can trigger autoscale events for a ec2 autoscalegroup.
func (n *chainnode) Ec2Autoscale() *Ec2AutoscaleNode {
	k := newEc2AutoscaleNode(n.Provides())
	n.linkChild(k)
	return k
}

// Create a node that tracks duration in a given state.
func (n *chainnode) StateDuration(expression *ast.LambdaNode) *StateDurationNode {
	sd := newStateDurationNode(n.provides, expression)
	n.linkChild(sd)
	return sd
}

// Create a node that tracks number of consecutive points in a given state.
func (n *chainnode) StateCount(expression *ast.LambdaNode) *StateCountNode {
	sc := newStateCountNode(n.provides, expression)
	n.linkChild(sc)
	return sc
}

// Create a node that can load data from external sources
func (n *chainnode) Sideload() *SideloadNode {
	s := newSideloadNode(n.provides)
	n.linkChild(s)
	return s
}
