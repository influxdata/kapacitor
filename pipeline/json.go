package pipeline

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/kapacitor/tick/ast"
)

const (
	// NodeTypeOf is used by all Node to identify the node duration Marshal and Unmarshal
	NodeTypeOf = "typeOf"
	// NodeID is used by all Node as a unique ID duration Marshal and Unmarshal
	NodeID = "id"
)

// TypeOf is a helper struct to add type information for each pipeline node
type TypeOf struct {
	Type string `json:"typeOf"`
	ID   ID     `json:"id,string"`
}

// Edge is a connection between a parent ID and a ChildID
type Edge struct {
	Parent ID `json:"parent,string"`
	Child  ID `json:"child,string"`
}

// JSONPipeline is the JSON serialization format for Pipeline
type JSONPipeline struct {
	Nodes []json.RawMessage `json:"nodes"`
	Edges []Edge            `json:"edges"`
}

type Graph map[ID][]ID
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

// JSONNode contains all fields associated with a node.  `typeOf`
// is used to determine which type of node this is.
type JSONNode map[string]interface{}

// NewJSONNode decodes JSON bytes into a JSONNode
func NewJSONNode(data []byte) (JSONNode, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var input JSONNode
	err := dec.Decode(&input)
	return input, err
}

// CheckTypeOf tests that the typeOf field is correctly set to typ.
func (j JSONNode) CheckTypeOf(typ string) error {
	t, ok := j[NodeTypeOf]
	if !ok {
		return fmt.Errorf("missing typeOf field")
	}

	if t != typ {
		return fmt.Errorf("error unmarshaling node type %s; received %s", typ, t)
	}
	return nil
}

// SetType adds the Node type information
func (j JSONNode) SetType(typ string) JSONNode {
	j[NodeTypeOf] = typ
	return j
}

// SetID adds the Node ID information
func (j JSONNode) SetID(id ID) JSONNode {
	j[NodeID] = fmt.Sprintf("%d", id)
	return j
}

// Set adds the key/value to the JSONNode
func (j JSONNode) Set(key string, value interface{}) JSONNode {
	j[key] = value
	return j
}

// SetDuration adds key to the JSONNode but formats the duration in InfluxQL style
func (j JSONNode) SetDuration(key string, value time.Duration) JSONNode {
	return j.Set(key, influxql.FormatDuration(value))
}

// Field returns expected field or error if field doesn't exist
func (j JSONNode) Field(field string) (interface{}, error) {
	fld, ok := j[field]
	if !ok {
		return nil, fmt.Errorf("missing expected field %s", field)
	}
	return fld, nil
}

// String reads the field for a string value
func (j JSONNode) String(field string) (string, error) {
	s, err := j.Field(field)
	if err != nil {
		return "", err
	}

	str, ok := s.(string)
	if !ok {
		return "", fmt.Errorf("field %s is not a string value but is %T", field, s)
	}
	return str, nil
}

// Int64 reads the field for a int64 value
func (j JSONNode) Int64(field string) (int64, error) {
	n, err := j.Field(field)
	if err != nil {
		return 0, err
	}

	jnum, ok := n.(json.Number)
	if ok {
		return jnum.Int64()
	}
	num, ok := n.(int64)
	if ok {
		return num, nil
	}
	return 0, fmt.Errorf("field %s is not an integer value but is %T", field, n)
}

// Float64 reads the field for a float64 value
func (j JSONNode) Float64(field string) (float64, error) {
	n, err := j.Field(field)
	if err != nil {
		return 0, err
	}

	jnum, ok := n.(json.Number)
	if ok {
		return jnum.Float64()
	}
	num, ok := n.(float64)
	if ok {
		return num, nil
	}
	return 0, fmt.Errorf("field %s is not a floating point value but is %T", field, n)
}

// Strings reads the field an array of strings
func (j JSONNode) Strings(field string) ([]string, error) {
	s, err := j.Field(field)
	if err != nil {
		return nil, err
	}

	strs, ok := s.([]string)
	if !ok {
		return nil, fmt.Errorf("field %s is not an array of strings but is %T", field, s)
	}
	return strs, nil
}

// Duration reads the field and assumes the string is in InfluxQL Duration format.
func (j JSONNode) Duration(field string) (time.Duration, error) {
	d, err := j.Field(field)
	if err != nil {
		return 0, err
	}

	dur, ok := d.(string)
	if !ok {
		return 0, fmt.Errorf("field %s is not a string duration value but is %T", field, d)
	}

	return influxql.ParseDuration(dur)
}

// Bool reads the field for a boolean value
func (j JSONNode) Bool(field string) (bool, error) {
	b, err := j.Field(field)
	if err != nil {
		return false, err
	}

	boolean, ok := b.(bool)
	if !ok {
		return false, fmt.Errorf("field %s is not a bool value but is %T", field, b)
	}
	return boolean, nil
}

// Lambda reads the field as an ast.LambdaNode
func (j JSONNode) Lambda(field string) (*ast.LambdaNode, error) {
	n, err := j.Field(field)
	if err != nil {
		return nil, err
	}

	if n == nil {
		return nil, nil
	}

	lamb, ok := n.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("field %s is not a lambda expression but is %T", field, n)
	}

	lambda := &ast.LambdaNode{}
	err = lambda.Unmarshal(lamb)
	return lambda, err
}

// Type returns the Node type.  This name can be used
// as the chain function name for the parent node.
func (j JSONNode) Type() (string, error) {
	return j.String(NodeTypeOf)
}

// ID returns the unique ID for this node.  This ID is used
// as the id of the parent and children in the Edges structure.
func (j JSONNode) ID() (ID, error) {
	i, err := j.String(NodeID)
	if err != nil {
		return 0, err
	}
	id, err := strconv.Atoi(i)
	return ID(id), err
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
		"holtWintersFit": func(parent chainnodeAlias, field string) *InfluxQLNode {
			return parent.HoltWintersWithFit(field, 0, 0, 0)
		},
	}
}

// IRNode is the intermediate representation of a node between JSON unmarshaling and a pipeline Node
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
		chainParent, ok := parent.(chainnodeAlias)
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
		chainParent, ok := parent.(chainnodeAlias)
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
		chainParent, ok := parent.(chainnodeAlias)
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
	switch typ.Type {
	case "top", "bottom":
		return unmarshalTopBottom(data, parents, typ)
	case "where":
		return unmarshalWhere(data, parents, typ)
	case "groupBy":
		return unmarshalGroupby(data, parents, typ)
	case "udf":
		return unmarshalUDF(data, parents, typ)
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
	chainParent, ok := parent.(chainnodeAlias)
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
