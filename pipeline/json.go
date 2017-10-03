package pipeline

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/influxdata/kapacitor/tick"
)

// TypeOf is a helper struct to add type information for each pipeline node
type TypeOf struct {
	Type string `json:"typeOf"`
	ID   ID     `json:"id,string"`
}

// Marshal will be removed
func Marshal(n Node) ([]byte, error) {
	switch node := n.(type) {
	case *WindowNode:
		type _WindowNode WindowNode
		return json.Marshal(struct {
			*WindowNode
			TypeOf
		}{
			WindowNode: node,
			TypeOf: TypeOf{
				Type: "window",
				ID:   n.ID(),
			},
		})

	}
	return nil, nil
}

// Edge is a connection between a parent ID and a ChildID
type Edge struct {
	Parent string `json:"parent"`
	Child  string `json:"child"`
}

// JSONPipeline is the JSON serialization format for Pipeline
type JSONPipeline struct {
	Nodes    []*JSONNode `json:"nodes"`
	Edges    []Edge      `json:"edges"`
	ids      map[string]*JSONNode
	types    map[string]string
	srcs     map[string]Node
	stats    []string
	sorted   []string
	parents  map[string][]string
	children map[string][]string
}

// Unmarshal will decode JSON structure into a cache of maps
func (j *JSONPipeline) Unmarshal(data []byte, v interface{}) error {
	type _j struct {
		*JSONPipeline
	}
	// Prevent recursion by creating a fake type
	if err := json.Unmarshal(data, _j{j}); err != nil {
		return err
	}
	return j.cache()
}

func (j *JSONPipeline) cache() error {
	for _, node := range j.Nodes {
		typ, err := node.Type()
		if err != nil {
			return err
		}
		id, err := node.ID()
		if err != nil {
			return err
		}

		j.ids[id] = node
		j.types[id] = typ

		if src, ok := node.IsSource(typ); ok {
			j.srcs[id] = src
		}

		if node.IsStat(typ) {
			j.stats = append(j.stats, id)
		}
	}

	j.toGraph()

	sorter := PipelineSorter{
		Graph: j.parents,
	}
	var err error
	j.sorted, err = sorter.Sort()
	return err
}

func (j *JSONPipeline) Parents(n string) []string {
	return j.children[n]
}

func (j *JSONPipeline) toGraph() {
	for _, edge := range j.Edges {
		parent, ok := j.parents[edge.Parent]
		if !ok {
			parent = []string{}
		}
		parent = append(parent, edge.Child)
		j.parents[edge.Parent] = parent

		child, ok := j.children[edge.Child]
		if !ok {
			child = []string{}
		}
		child = append(child, edge.Parent)
		j.children[edge.Child] = child
	}
}

// Sources returns all source nodes in the raw JSONPipeline
func (j *JSONPipeline) Sources() map[string]Node {
	return j.srcs
}

func (j *JSONPipeline) Sorted() []string {
	return j.sorted
}

type PipelineSorter struct {
	Graph     map[string][]string
	permanent map[string]bool
	temporary map[string]bool
	sorted    []string
}

func (p *PipelineSorter) Sort() ([]string, error) {
	for n := range p.Graph {
		if err := p.visit(n); err != nil {
			return nil, err
		}
	}
	return p.sorted, nil
}

func (p *PipelineSorter) visit(node string) error {
	if _, marked := p.permanent[node]; marked {
		return nil
	}
	if _, marked := p.temporary[node]; marked {
		return fmt.Errorf("cycle detected. kapacitor pipelines must not have cycles")
	}
	p.temporary[node] = true
	children := p.Graph[node]
	for _, child := range children {
		p.visit(child)
	}
	p.permanent[node] = true
	p.sorted = append([]string{node}, p.sorted...)
	return nil
}

// JSONNode contains all fields associated with a node.  `typeOf`
// is used to determine which type of node this is.
type JSONNode map[string]interface{}

// Type returns the Node type.  This name can be used
// as the chain function name for the parent node.
func (j JSONNode) Type() (string, error) {
	t, ok := j["typeOf"]
	if !ok {
		return "", fmt.Errorf("node requires typeOf")
	}
	typ, ok := t.(string)
	if !ok {
		return "", fmt.Errorf("typeOf must be string but is %T", t)
	}
	return typ, nil
}

// ID returns the unique ID for this node.  This ID is used
// as the id of the parent and children in the Edges structure.
func (j JSONNode) ID() (string, error) {
	i, ok := j["id"]
	if !ok {
		return "", fmt.Errorf("node requires id")
	}
	id, ok := i.(string)
	if !ok {
		return "", fmt.Errorf("id must be string but is %T", i)
	}
	return id, nil
}

// IsSource returns the Stream or BatchNode if this is a stream or batch.
func (j JSONNode) IsSource(typ string) (Node, bool) {
	switch typ {
	case "stream":
		return newStreamNode(), true
	case "batch":
		return newBatchNode(), true
	default:
		return nil, false
	}
}

// IsStat returns true if this node is a stat node.
func (j JSONNode) IsStat(typ string) bool {
	if typ == "stats" {
		return true
	}
	return false
}

// Unmarshal deserializes the pipeline from JSON.
func (p *Pipeline) Unmarshal(data []byte, v interface{}) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var input JSONPipeline
	if err := dec.Decode(&input); err != nil {
		return err
	}
	srcs := input.Sources()
	for _, src := range srcs {
		p.addSource(src)
	}

	nodes := map[string]Node{}
	for k, v := range srcs {
		nodes[k] = v
	}

	sorted := input.Sorted()
	for _, n := range sorted {
		// Because sources have been prepopulated into the pipeline
		// we can skip them here.
		if _, ok := srcs[n]; ok {
			continue
		}
		parents := input.Parents(n)
		if len(parents) == 0 {
			return fmt.Errorf("Node ID %s requires at least one parent", n)
		}
		root := parents[0]
		node, ok := input.ids[n]
		if !ok {
			return fmt.Errorf("Node ID %s has edge but no node body")
		}
		typ, err := node.Type()
		if err != nil {
			return err
		}
		rRoot, err := tick.NewReflectionDescriber(root, nil)
		if err != nil {
			return err
		}
		rRoot.CallChainMethod

	}

	// All sources have
	for _, src := range srcs {
		rSrc, err := tick.NewReflectionDescriber(src, nil)
		if err != nil {
			return err
		}
		rSrc.CallChainMethod("howdy", 1)
	}
	return nil
}

/*
func (p *Pipeline) Unmarshal(data []byte, v interface{}) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var pipe JSONPipeline
	if err := dec.Decode(&pipe); err != nil {
		return err
	}

	ids := map[ID]*JSONNode{}
	types := map[ID]string{}
	srcs := map[ID]Node{}
	stats := []ID{}
	for _, node := range pipe.Nodes {
		typ, err := node.Type()
		if err != nil {
			return err
		}
		id, err := node.ID()
		if err != nil {
			return err
		}

		ids[id] = node
		types[id] = typ

		if src, ok := node.IsSource(typ); ok {
			srcs[id] = src
		}

		if node.IsStat(typ) {
			stats = append(stats, id)
		}
	}

	pipeSrcs := []Node{}
	for _, s := range srcs {
		pipeSrcs = append(pipeSrcs, s)
	}
	p = CreatePipelineSources(pipeSrcs...)
	pGraph, cGraph := graph(pipe.Edges)

	for _, s := range srcs {
		childNodes := pGraph[s]
	}

	return nil
}

type Graph map[string][]string

func graph(edges []Edge) (parents Graph, children Graph) {
	parents = Graph{}
	children = Graph{}
	for _, edge := range edges {
		parent, ok := parents[edge.Parent]
		if !ok {
			parent = []string{}
		}
		parent = append(parent, edge.Child)
		parents[edge.Parent] = parent

		child, ok := children[edge.Child]
		if !ok {
			child = []string{}
		}
		child = append(child, edge.Parent)
		children[edge.Child] = child
	}
	return parents, children
}
*/
