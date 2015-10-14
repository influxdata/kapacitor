package pipeline

import (
	"bytes"
	"fmt"
)

// The type of data that travels along an edge connecting two nodes in a Pipeline.
type EdgeType int

const (
	// No data is transfered
	NoEdge EdgeType = iota
	// Data is transfered immediately and one point at a time.
	StreamEdge
	// Data is transfered in batches as soon as it is ready.
	BatchEdge
	// Data is transfered as it is received from a map function.
	ReduceEdge
)

type ID int

func (e EdgeType) String() string {
	switch e {
	case StreamEdge:
		return "stream"
	case BatchEdge:
		return "batch"
	case ReduceEdge:
		return "reduce"
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

	// Return .dot string to graph DAG
	dot(buf *bytes.Buffer)
}

type node struct {
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

func (n *node) Desc() string {
	return n.desc
}

func (n *node) ID() ID {
	return n.id
}

func (n *node) setID(id ID) {
	n.id = id
}

func (n *node) Name() string {
	if n.name == "" {
		n.name = fmt.Sprintf("%s%d", n.Desc(), n.ID())
	}
	return n.name
}

func (n *node) SetName(name string) {
	n.name = name
}

func (n *node) Parents() []Node {
	return n.parents
}

func (n *node) Children() []Node {
	return n.children
}

func (n *node) addParent(c Node) {
	n.parents = append(n.parents, c)
}

func (n *node) linkChild(c Node) {
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

func (n *node) Wants() EdgeType {
	return n.wants
}

func (n *node) Provides() EdgeType {
	return n.provides
}

func (n *node) dot(buf *bytes.Buffer) {
	for _, c := range n.children {
		buf.Write([]byte(fmt.Sprintf("%s -> %s;\n", n.Name(), c.Name())))
	}
}

// ---------------------------------
// Chaining methods
//

func (n *node) Where(predicate string) Node {
	w := newWhereNode(n.provides, predicate)
	n.linkChild(w)
	return w
}

func (n *node) Map(f interface{}, field string) (c Node) {
	switch n.provides {
	case StreamEdge:
		panic("cannot MapReduce stream edge, did you forget to window the data?")
	case BatchEdge:
		c = NewMapNode(f, field)
	}
	n.linkChild(c)
	return c
}

func (n *node) Reduce(f interface{}) (c Node) {
	switch n.provides {
	case StreamEdge:
		panic("cannot MapReduce stream edge, did you forget to window the data?")
	case BatchEdge:
		c = NewReduceNode(f)
	}
	n.linkChild(c)
	return c
}

func (n *node) MapReduce(f MapReduceFunc, field string) Node {
	var m Node
	var r Node
	mf, rf := f()
	switch n.provides {
	case StreamEdge:
		panic("cannot MapReduce stream edge, did you forget to window the data?")
	case BatchEdge:
		m = NewMapNode(mf, field)
		r = NewReduceNode(rf)
	}
	n.linkChild(m)
	m.linkChild(r)
	return r
}

// Create a subnode that windows the stream by time.
func (n *node) Window() *WindowNode {
	w := newWindowNode()
	n.linkChild(w)
	return w
}

// Create subnode that contains a cache of the most recent window.
// The endpoint is the relative path in the API where the cached data will be exposed.
func (n *node) HttpOut(endpoint string) *HTTPOutNode {
	h := newHTTPOutNode(n.provides, endpoint)
	n.linkChild(h)
	return h
}

// Create subnode that will store the incoming data into InfluxDB
func (n *node) InfluxDBOut() *InfluxDBOutNode {
	i := newInfluxDBOutNode(n.provides)
	n.linkChild(i)
	return i
}

func (n *node) Alert() *AlertNode {
	a := newAlertNode(n.provides)
	n.linkChild(a)
	return a
}

func (n *node) Union(ns ...Node) *UnionNode {
	u := newUnionNode(n.provides, ns)
	n.linkChild(u)
	return u
}

func (n *node) Join(o Node) *JoinNode {
	j := newJoinNode(n.provides, o)
	n.linkChild(j)
	return j
}

func (n *node) Apply(f interface{}) Node {
	a := newApplyNode(n.provides, f)
	n.linkChild(a)
	return a
}

// Group the data by a set of dimensions.
func (n *node) GroupBy(ds ...interface{}) Node {
	dims := make([]string, len(ds))
	for i, d := range ds {
		str, ok := d.(string)
		if !ok {
			panic("GroupBy dimensions must be strings")
		}
		dims[i] = str
	}
	g := newGroupByNode(n.provides, dims)
	n.linkChild(g)
	return g
}
