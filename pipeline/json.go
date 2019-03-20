package pipeline

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/influxdata/kapacitor/tick/ast"
)

const (
	// NodeTypeOf is used by all Node to identify the node duration Marshal and Unmarshal
	NodeTypeOf = "typeOf"
	// NodeID is used by all Node as a unique ID duration Marshal and Unmarshal
	NodeID = "id"
)

// TypeOf is a helper struct to add type information for each pipeline node
// tick:ignore
type TypeOf struct {
	Type string `json:"typeOf"`
	ID   ID     `json:"id,string"`
}

// Edge is a connection between a parent ID and a ChildID
// tick:ignore
type Edge struct {
	Parent ID `json:"parent,string"`
	Child  ID `json:"child,string"`
}

// JSONPipeline is the JSON serialization format for Pipeline
// tick:ignore
type JSONPipeline struct {
	Nodes []json.RawMessage `json:"nodes"`
	Edges []Edge            `json:"edges"`
}

// Graph represents a series of directional edges
type Graph map[ID][]ID

// PipelineSorter performs topological sort on the Edges of JSONPipeline.
// tick:ignore
type PipelineSorter struct {
	Edges         []Edge
	ChildrenOf    Graph
	ParentsOf     Graph
	SortedIndexes map[ID]int
	sorted        []ID

	permanent map[ID]bool
	temporary map[ID]bool
}

func NewPipelineSorter(edges []Edge) *PipelineSorter {
	return &PipelineSorter{
		Edges:         edges,
		ChildrenOf:    Graph{},
		ParentsOf:     Graph{},
		SortedIndexes: map[ID]int{},
		permanent:     map[ID]bool{},
		temporary:     map[ID]bool{},
	}
}

func (p *PipelineSorter) Graphs() (childrenOf Graph, parentsOf Graph) {
	if len(p.ChildrenOf) == 0 || len(p.ParentsOf) == 0 {
		childrenOf := Graph{}
		parentsOf := Graph{}
		for _, edge := range p.Edges {
			children, ok := childrenOf[edge.Parent]
			if !ok {
				children = []ID{}
			}
			children = append(children, edge.Child)
			childrenOf[edge.Parent] = children

			parents, ok := parentsOf[edge.Child]
			if !ok {
				parents = []ID{}
			}
			parents = append(parents, edge.Parent)
			parentsOf[edge.Child] = parents
		}
		p.ChildrenOf = childrenOf
		p.ParentsOf = parentsOf
	}
	return p.ChildrenOf, p.ParentsOf
}

func (p *PipelineSorter) Sort() (map[ID]int, error) {
	childrenOf, _ := p.Graphs()
	for n := range childrenOf {
		if err := p.visit(n, childrenOf); err != nil {
			return nil, err
		}
	}
	for i, id := range p.sorted {
		p.SortedIndexes[id] = i
	}
	return p.SortedIndexes, nil
}

func (p *PipelineSorter) visit(node ID, childrenOf Graph) error {
	if _, marked := p.permanent[node]; marked {
		return nil
	}
	if _, marked := p.temporary[node]; marked {
		return fmt.Errorf("cycle detected. kapacitor pipelines must not have cycles")
	}
	p.temporary[node] = true
	children := childrenOf[node]
	for _, child := range children {
		p.visit(child, childrenOf)
	}
	p.permanent[node] = true
	p.sorted = append([]ID{node}, p.sorted...)
	return nil
}

func (p *Pipeline) MarshalJSON() ([]byte, error) {
	if p.sorted == nil {
		p.sort()
	}
	var raw = &struct {
		Nodes []Node `json:"nodes"`
		Edges []Edge `json:"edges"`
	}{}

	for _, n := range p.sorted {
		// we skip all noop nodes
		if _, ok := n.(*NoOpNode); ok {
			continue
		}

		// With a stats node we "fake" a parent to hook it correctly into the graph
		if stat, ok := n.(*StatsNode); ok {
			raw.Edges = append(raw.Edges,
				Edge{
					Parent: stat.SourceNode.ID(),
					Child:  stat.ID(),
				})
		}

		raw.Nodes = append(raw.Nodes, n)
		for _, parent := range n.Parents() {
			raw.Edges = append(raw.Edges,
				Edge{
					Parent: parent.ID(),
					Child:  n.ID(),
				})
		}
	}
	return json.Marshal(raw)
}

var chainFunctions map[string]func(parent chainnodeAlias) Node
var sourceFunctions map[string]func() Node
var sourceFilters map[string]func([]byte, Node) (Node, error)
var multiParents map[string]func(chainnodeAlias, []Node) Node
var influxFunctions map[string]func(chainnodeAlias, string) *InfluxQLNode
var uniqFunctions map[string]func([]byte, []Node, TypeOf) (Node, error)

func init() {
	// Add all possible sources
	sourceFunctions = map[string]func() Node{
		"stream": func() Node { return newStreamNode() },
		"batch":  func() Node { return newBatchNode() },
	}

	// Filters modify the source and produce a specific data stream
	sourceFilters = map[string]func([]byte, Node) (Node, error){
		"from":  unmarshalFrom,
		"query": unmarshalQuery,
	}

	// Add default construction of chain nodes
	chainFunctions = map[string]func(parent chainnodeAlias) Node{
		"window":            func(parent chainnodeAlias) Node { return parent.Window() },
		"swarmAutoscale":    func(parent chainnodeAlias) Node { return parent.SwarmAutoscale() },
		"stats":             func(parent chainnodeAlias) Node { return parent.Stats(0) },
		"stateDuration":     func(parent chainnodeAlias) Node { return parent.StateDuration(nil) },
		"stateCount":        func(parent chainnodeAlias) Node { return parent.StateCount(nil) },
		"shift":             func(parent chainnodeAlias) Node { return parent.Shift(0) },
		"sideload":          func(parent chainnodeAlias) Node { return parent.Sideload() },
		"sample":            func(parent chainnodeAlias) Node { return parent.Sample(0) },
		"log":               func(parent chainnodeAlias) Node { return parent.Log() },
		"kapacitorLoopback": func(parent chainnodeAlias) Node { return parent.KapacitorLoopback() },
		"k8sAutoscale":      func(parent chainnodeAlias) Node { return parent.K8sAutoscale() },
		"influxdbOut":       func(parent chainnodeAlias) Node { return parent.InfluxDBOut() },
		"httpPost":          func(parent chainnodeAlias) Node { return parent.HttpPost() },
		"httpOut":           func(parent chainnodeAlias) Node { return parent.HttpOut("") },
		"flatten":           func(parent chainnodeAlias) Node { return parent.Flatten() },
		"eval":              func(parent chainnodeAlias) Node { return parent.Eval() },
		"derivative":        func(parent chainnodeAlias) Node { return parent.Derivative("") },
		"changeDetect":      func(parent chainnodeAlias) Node { return parent.ChangeDetect("") },
		"delete":            func(parent chainnodeAlias) Node { return parent.Delete() },
		"default":           func(parent chainnodeAlias) Node { return parent.Default() },
		"combine":           func(parent chainnodeAlias) Node { return parent.Combine(nil) },
		"alert":             func(parent chainnodeAlias) Node { return parent.Alert() },
	}

	multiParents = map[string]func(chainnodeAlias, []Node) Node{
		"union": func(parent chainnodeAlias, nodes []Node) Node { return parent.Union(nodes...) },
		"join":  func(parent chainnodeAlias, nodes []Node) Node { return parent.Join(nodes...) },
	}

	influxFunctions = map[string]func(chainnodeAlias, string) *InfluxQLNode{
		"count":         func(parent chainnodeAlias, field string) *InfluxQLNode { return parent.Count(field) },
		"distinct":      func(parent chainnodeAlias, field string) *InfluxQLNode { return parent.Distinct(field) },
		"mean":          func(parent chainnodeAlias, field string) *InfluxQLNode { return parent.Mean(field) },
		"median":        func(parent chainnodeAlias, field string) *InfluxQLNode { return parent.Median(field) },
		"mode":          func(parent chainnodeAlias, field string) *InfluxQLNode { return parent.Mode(field) },
		"spread":        func(parent chainnodeAlias, field string) *InfluxQLNode { return parent.Spread(field) },
		"sum":           func(parent chainnodeAlias, field string) *InfluxQLNode { return parent.Sum(field) },
		"first":         func(parent chainnodeAlias, field string) *InfluxQLNode { return parent.First(field) },
		"last":          func(parent chainnodeAlias, field string) *InfluxQLNode { return parent.Last(field) },
		"min":           func(parent chainnodeAlias, field string) *InfluxQLNode { return parent.Min(field) },
		"max":           func(parent chainnodeAlias, field string) *InfluxQLNode { return parent.Max(field) },
		"stddev":        func(parent chainnodeAlias, field string) *InfluxQLNode { return parent.Stddev(field) },
		"difference":    func(parent chainnodeAlias, field string) *InfluxQLNode { return parent.Difference(field) },
		"cumulativeSum": func(parent chainnodeAlias, field string) *InfluxQLNode { return parent.CumulativeSum(field) },
		"percentile":    func(parent chainnodeAlias, field string) *InfluxQLNode { return parent.Percentile(field, 0) },
		"elapsed":       func(parent chainnodeAlias, field string) *InfluxQLNode { return parent.Elapsed(field, 0) },
		"movingAverage": func(parent chainnodeAlias, field string) *InfluxQLNode { return parent.MovingAverage(field, 0) },
		"holtWinters":   func(parent chainnodeAlias, field string) *InfluxQLNode { return parent.HoltWinters(field, 0, 0, 0) },
		"holtWintersWithFit": func(parent chainnodeAlias, field string) *InfluxQLNode {
			return parent.HoltWintersWithFit(field, 0, 0, 0)
		},
	}

	uniqFunctions = map[string]func([]byte, []Node, TypeOf) (Node, error){
		"top":     unmarshalTopBottom,
		"bottom":  unmarshalTopBottom,
		"where":   unmarshalWhere,
		"groupBy": unmarshalGroupby,
		"udf":     unmarshalUDF,
	}
}

// IRNode is the intermediate representation of a node between JSON unmarshaling and a pipeline Node
// tick:ignore
type IRNode struct {
	Raw    json.RawMessage
	TypeOf TypeOf
	Pos    int
}

func (p *Pipeline) Unmarshal(data []byte) error {
	var raw JSONPipeline
	err := json.Unmarshal(data, &raw)
	if err != nil {
		return err
	}
	sorter := NewPipelineSorter(raw.Edges)
	_, parentsOf := sorter.Graphs()
	sorted, err := sorter.Sort()
	if err != nil {
		return err
	}

	// The intermediate representation is used to grab the type and id
	// information common to all nodes. Additionally, the slice can be
	// sorted using the positions of the nodes in the topo sort from above.
	irNodes := make([]IRNode, len(raw.Nodes))
	for i, rawNode := range raw.Nodes {
		if err := json.Unmarshal(rawNode, &irNodes[i].TypeOf); err != nil {
			return err
		}
		id := irNodes[i].TypeOf.ID
		pos, ok := sorted[id]
		if !ok {
			return fmt.Errorf("node %d not listed in edges", id)
		}
		irNodes[i].Pos = pos
		irNodes[i].Raw = rawNode
	}

	sort.Slice(irNodes, func(i, j int) bool {
		return irNodes[i].Pos < irNodes[j].Pos
	})

	nodes := map[ID]Node{} // used to lookup nodes that have been fully deserialized
	// Need to go through sort order to guarantee that parents have been constructed before children
	for _, irNode := range irNodes {
		parentIDs := parentsOf[irNode.TypeOf.ID]
		parents := make([]Node, len(parentIDs))
		for i, id := range parentIDs {
			parent, ok := nodes[id]
			if !ok {
				return fmt.Errorf("parent %d of node %d not yet created", id, irNode.TypeOf.ID)
			}
			parents[i] = parent
		}
		node, err := p.unmarshalNode(irNode.Raw, irNode.TypeOf, parents)
		if err != nil {
			return err
		}
		nodes[irNode.TypeOf.ID] = node
	}
	p.sort()
	return nil
}

func (p *Pipeline) unmarshalNode(data []byte, typ TypeOf, parents []Node) (Node, error) {
	src, ok := sourceFunctions[typ.Type]
	if ok {
		if len(parents) != 0 {
			return nil, fmt.Errorf("expected no parents for source node %d but found %d", typ.ID, len(parents))
		}
		s := src()
		p.addSource(s)
		return s, nil
	}

	fn, ok := chainFunctions[typ.Type]
	if ok {
		if len(parents) != 1 {
			return nil, fmt.Errorf("expected one parent for node %d but found %d", typ.ID, len(parents))
		}
		parent := parents[0]
		chainParent, ok := isChainNode(parent)
		if !ok {
			return nil, fmt.Errorf("parent node is not a chain node but is %T", parent)
		}
		child := fn(chainParent)
		err := json.Unmarshal(data, child)
		return child, err
	}

	filter, ok := sourceFilters[typ.Type]
	if ok {
		if len(parents) != 1 {
			return nil, fmt.Errorf("expected one parent for node %d but found %d", typ.ID, len(parents))
		}
		parent := parents[0]
		return filter(data, parent)
	}

	merge, ok := multiParents[typ.Type]
	if ok {
		if len(parents) < 2 {
			return nil, fmt.Errorf("expected more than one parent for node %d but received %d", typ.ID, len(parents))
		}
		parent := parents[0]
		chainParent, ok := isChainNode(parent)
		if !ok {
			return nil, fmt.Errorf("parent node is not a chain node but is %T", parent)
		}
		child := merge(chainParent, parents[1:])
		err := json.Unmarshal(data, child)
		return child, err
	}

	infn, ok := influxFunctions[typ.Type]
	if ok {
		if len(parents) != 1 {
			return nil, fmt.Errorf("expected one parent for node %d but found %d", typ.ID, len(parents))
		}
		parent := parents[0]
		chainParent, ok := isChainNode(parent)
		if !ok {
			return nil, fmt.Errorf("parent node is not a chain node but is %T", parent)
		}

		var raw = &struct {
			Field string `json:"field"`
		}{}
		err := json.Unmarshal(data, raw)
		if err != nil {
			return nil, err
		}
		child := infn(chainParent, raw.Field)
		child.Method = typ.Type
		err = json.Unmarshal(data, child)
		if err != nil {
			return nil, err
		}
		return child, nil
	}

	uniq, ok := uniqFunctions[typ.Type]
	if ok {
		return uniq(data, parents, typ)
	}
	return nil, fmt.Errorf("unknown function type %s for node %d", typ.Type, typ.ID)
}

func unmarshalFrom(data []byte, source Node) (Node, error) {
	stream, ok := source.(*StreamNode)
	if !ok {
		return nil, fmt.Errorf("parent of query node must be a StreamNode but is %T", source)
	}
	child := stream.From()
	err := json.Unmarshal(data, child)
	return child, err
}

func unmarshalQuery(data []byte, source Node) (Node, error) {
	batch, ok := source.(*BatchNode)
	if !ok {
		return nil, fmt.Errorf("parent of query node must be a BatchNode but is %T", source)
	}
	child := batch.Query("")
	err := json.Unmarshal(data, child)
	return child, err
}

func unmarshalWhere(data []byte, parents []Node, typ TypeOf) (Node, error) {
	if len(parents) != 1 {
		return nil, fmt.Errorf("expected one parent for node %d but found %d", typ.ID, len(parents))
	}
	parent := parents[0]
	nodeParent, ok := parent.(chainNodeAliasWhere)
	if !ok {
		return nil, fmt.Errorf("parent node does not have where clause but is %T", parent)
	}
	child := nodeParent.Where(nil)
	err := json.Unmarshal(data, child)
	return child, err
}

func unmarshalGroupby(data []byte, parents []Node, typ TypeOf) (Node, error) {
	if len(parents) != 1 {
		return nil, fmt.Errorf("expected one parent for node %d but found %d", typ.ID, len(parents))
	}
	parent := parents[0]
	nodeParent, ok := parent.(chainNodeAliasGroupBy)
	if !ok {
		return nil, fmt.Errorf("parent node does not have groupBy clause but is %T", parent)
	}
	child := nodeParent.GroupBy()
	err := json.Unmarshal(data, child)
	return child, err
}

func unmarshalStats(data []byte, parents []Node, typ TypeOf) (Node, error) {
	if len(parents) != 1 {
		return nil, fmt.Errorf("expected one parent for node %d but found %d", typ.ID, len(parents))
	}
	parent := parents[0]
	nodeParent, ok := parent.(*node)
	if !ok {
		return nil, fmt.Errorf("parent node is not a node but is %T", parent)
	}
	child := nodeParent.Stats(0)
	err := json.Unmarshal(data, child)
	return child, err
}

func unmarshalTopBottom(data []byte, parents []Node, typ TypeOf) (Node, error) {
	if len(parents) != 1 {
		return nil, fmt.Errorf("expected one parent for node %d but found %d", typ.ID, len(parents))
	}
	parent := parents[0]
	chainParent, ok := isChainNode(parent)
	if !ok {
		return nil, fmt.Errorf("parent node is not a chain node but is %T", parent)
	}

	var raw = &struct {
		Field string   `json:"field"`
		Tags  []string `json:"tags"`
	}{}
	err := json.Unmarshal(data, raw)
	if err != nil {
		return nil, err
	}
	var child *InfluxQLNode
	switch typ.Type {
	case "top":
		child = chainParent.Top(0, raw.Field, raw.Tags...)
	case "bottom":
		child = chainParent.Bottom(0, raw.Field, raw.Tags...)
	default:
		return nil, fmt.Errorf("expected top or bottom node but found %s", typ.Type)
	}

	err = json.Unmarshal(data, child)
	return child, err
}

func unmarshalUDF(data []byte, parents []Node, typ TypeOf) (Node, error) {
	if len(parents) != 1 {
		return nil, fmt.Errorf("expected one parent for node %d but found %d", typ.ID, len(parents))
	}
	parent := parents[0]
	child := &UDFNode{}
	parent.linkChild(child)
	err := json.Unmarshal(data, child)
	return child, err
}

func isChainNode(node Node) (chainnodeAlias, bool) {
	a, ok := node.(chainnodeAlias)
	if ok {
		return a, ok
	}
	alert, ok := node.(*AlertNode)
	if ok {
		return &alert.AlertNodeData.chainnode, true
	}
	shift, ok := node.(*ShiftNode)
	if ok {
		return &shift.chainnode, true
	}
	return nil, false
}

// chainnodeAlias is used to check for the presence of a chain node
type chainnodeAlias interface {
	Alert() *AlertNode
	Bottom(int64, string, ...string) *InfluxQLNode
	Children() []Node
	Combine(...*ast.LambdaNode) *CombineNode
	Count(string) *InfluxQLNode
	CumulativeSum(string) *InfluxQLNode
	Deadman(float64, time.Duration, ...*ast.LambdaNode) *AlertNode
	Default() *DefaultNode
	Delete() *DeleteNode
	Derivative(string) *DerivativeNode
	ChangeDetect(...string) *ChangeDetectNode
	Desc() string
	Difference(string) *InfluxQLNode
	Distinct(string) *InfluxQLNode
	Elapsed(string, time.Duration) *InfluxQLNode
	Eval(...*ast.LambdaNode) *EvalNode
	First(string) *InfluxQLNode
	Flatten() *FlattenNode
	HoltWinters(string, int64, int64, time.Duration) *InfluxQLNode
	HoltWintersWithFit(string, int64, int64, time.Duration) *InfluxQLNode
	HttpOut(string) *HTTPOutNode
	HttpPost(...string) *HTTPPostNode
	ID() ID
	InfluxDBOut() *InfluxDBOutNode
	Join(...Node) *JoinNode
	K8sAutoscale() *K8sAutoscaleNode
	KapacitorLoopback() *KapacitorLoopbackNode
	Last(string) *InfluxQLNode
	Log() *LogNode
	Max(string) *InfluxQLNode
	Mean(string) *InfluxQLNode
	Median(string) *InfluxQLNode
	Min(string) *InfluxQLNode
	Mode(string) *InfluxQLNode
	MovingAverage(string, int64) *InfluxQLNode
	Name() string
	Parents() []Node
	Percentile(string, float64) *InfluxQLNode
	Provides() EdgeType
	Sample(interface{}) *SampleNode
	SetName(string)
	Shift(time.Duration) *ShiftNode
	Sideload() *SideloadNode
	Spread(string) *InfluxQLNode
	StateCount(*ast.LambdaNode) *StateCountNode
	StateDuration(*ast.LambdaNode) *StateDurationNode
	Stats(time.Duration) *StatsNode
	Stddev(string) *InfluxQLNode
	Sum(string) *InfluxQLNode
	SwarmAutoscale() *SwarmAutoscaleNode
	Top(int64, string, ...string) *InfluxQLNode
	Union(...Node) *UnionNode
	Wants() EdgeType
	Window() *WindowNode
	addParent(Node)
	dot(*bytes.Buffer)
	holtWinters(string, int64, int64, time.Duration, bool) *InfluxQLNode
	linkChild(Node)
	pMark() bool
	pipeline() *Pipeline
	setID(ID)
	setPMark(bool)
	setPipeline(*Pipeline)
	setTMark(bool)
	tMark() bool
	validate() error
}

// chainnodeAliasWhere exists because FromNode and chainnodes have different Where()
type chainNodeAliasWhere interface {
	Where(*ast.LambdaNode) *WhereNode
}

// chainNodeAliasGroupBy exists because FromNode, BatchNode and chainnodes have different GroupBy()
type chainNodeAliasGroupBy interface {
	GroupBy(...interface{}) *GroupByNode
}
