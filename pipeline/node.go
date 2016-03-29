package pipeline

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	"github.com/influxdata/kapacitor/tick"
)

// The type of data that travels along an edge connecting two nodes in a Pipeline.
type EdgeType int

const (
	// No data is transferred
	NoEdge EdgeType = iota
	// Data is transferred immediately and one point at a time.
	StreamEdge
	// Data is transferred in batches as soon as it is ready.
	BatchEdge
)

type ID int

func (e EdgeType) String() string {
	switch e {
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
	n.p.assignID(c)
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

// Helper function for creating an alert on low throughput, aka deadman's switch.
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
//        |derivative('collected')
//            .unit(10s)
//            .nonNegative()
//        |alert()
//            .id('node \'stream0\' in task \'{{ .TaskName }}\'')
//            .message('{{ .ID }} is {{ if eq .Level "OK" }}alive{{ else }}dead{{ end }}: {{ index .Fields "collected" | printf "%0.3f" }} points/10s.')
//            .crit(lamdba: "collected" <= 100.0)
//    //Do normal processing of data
//    data...
//
// The `id` and `message` alert properties can be configured globally via the 'deadman' configuration section.
//
// Since the AlertNode is the last piece it can be further modified as normal.
// Example:
//    var data = stream
//        |from()...
//    // Trigger critical alert if the throughput drops below 100 points per 1s and checked every 10s.
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
func (n *node) Deadman(threshold float64, interval time.Duration, expr ...tick.Node) *AlertNode {
	dn := n.Stats(interval).
		Derivative("emitted").NonNegative()
	dn.Unit = interval

	an := dn.Alert()
	critExpr := &tick.BinaryNode{
		Operator: tick.TokenLessEqual,
		Left: &tick.ReferenceNode{
			Reference: "emitted",
		},
		Right: &tick.NumberNode{
			IsFloat: true,
			Float64: threshold,
		},
	}
	// Add any additional expressions
	for _, e := range expr {
		critExpr = &tick.BinaryNode{
			Operator: tick.TokenAnd,
			Left:     critExpr,
			Right:    e,
		}
	}
	an.Crit = critExpr
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
func (n *chainnode) Where(expression tick.Node) *WhereNode {
	w := newWhereNode(n.provides, expression)
	n.linkChild(w)
	return w
}

// Create an http output node that caches the most recent data it has received.
// The cached data is available at the given endpoint.
// The endpoint is the relative path from the API endpoint of the running task.
// For example if the task endpoint is at "/api/v1/task/<task_name>" and endpoint is
// "top10", then the data can be requested from "/api/v1/task/<task_name>/top10".
func (n *chainnode) HttpOut(endpoint string) *HTTPOutNode {
	h := newHTTPOutNode(n.provides, endpoint)
	n.linkChild(h)
	return h
}

// Create an influxdb output node that will store the incoming data into InfluxDB.
func (n *chainnode) InfluxDBOut() *InfluxDBOutNode {
	i := newInfluxDBOutNode(n.provides)
	n.linkChild(i)
	return i
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

// Join this node with other nodes. The data is joined on timestamp.
func (n *chainnode) Join(others ...Node) *JoinNode {
	others = append([]Node{n}, others...)
	j := newJoinNode(n.provides, others)
	return j
}

// Create an eval node that will evaluate the given transformation function to each data point.
//  A list of expressions may be provided and will be evaluated in the order they are given
// and results of previous expressions are made available to later expressions.
func (n *chainnode) Eval(expressions ...tick.Node) *EvalNode {
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
