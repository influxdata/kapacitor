package pipeline

import (
	"bytes"
	"fmt"
	"time"

	"github.com/influxdata/kapacitor/tick"
	"github.com/influxdata/kapacitor/tick/stateful"
)

// Information relavant to configuring a deadman's swith
type DeadmanService interface {
	Interval() time.Duration
	Threshold() float64
	Id() string
	Message() string
	Global() bool
}

// Create a template pipeline
// tick:ignore
func CreateTemplatePipeline(
	script string,
	sourceEdge EdgeType,
	scope *stateful.Scope,
	deadman DeadmanService,
) (*TemplatePipeline, error) {
	p, vars, err := createPipelineAndVars(script, sourceEdge, scope, deadman, nil, true)
	if err != nil {
		return nil, err
	}
	tp := &TemplatePipeline{
		p:    p,
		vars: vars,
	}
	return tp, nil
}

// Create a pipeline from a given script.
// tick:ignore
func CreatePipeline(
	script string,
	sourceEdge EdgeType,
	scope *stateful.Scope,
	deadman DeadmanService,
	predefinedVars map[string]tick.Var,
) (*Pipeline, error) {
	p, _, err := createPipelineAndVars(script, sourceEdge, scope, deadman, predefinedVars, false)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func createPipelineAndVars(
	script string,
	sourceEdge EdgeType,
	scope *stateful.Scope,
	deadman DeadmanService,
	predefinedVars map[string]tick.Var,
	ignoreMissingVars bool,
) (*Pipeline, map[string]tick.Var, error) {
	p := &Pipeline{
		deadman: deadman,
	}
	var src Node
	switch sourceEdge {
	case StreamEdge:
		src = newStreamNode()
		scope.Set("stream", src)
	case BatchEdge:
		src = newBatchNode()
		scope.Set("batch", src)
	default:
		return nil, nil, fmt.Errorf("source edge type must be either Stream or Batch not %s", sourceEdge)
	}
	p.addSource(src)

	vars, err := tick.Evaluate(script, scope, predefinedVars, ignoreMissingVars)
	if err != nil {
		return nil, nil, err
	}
	if deadman.Global() {
		switch s := src.(type) {
		case *StreamNode:
			s.Deadman(deadman.Threshold(), deadman.Interval())
		case *BatchNode:
			s.Deadman(deadman.Threshold(), deadman.Interval())
		default:
			return nil, nil, fmt.Errorf("source edge type must be either Stream or Batch not %s", sourceEdge)
		}
	}
	if err = p.Walk(
		func(n Node) error {
			return n.validate()
		}); err != nil {
		return nil, nil, err
	}
	return p, vars, nil
}

// A complete data processing pipeline. Starts with a single source.
// tick:ignore
type Pipeline struct {
	sources []Node
	id      ID
	sorted  []Node

	deadman DeadmanService
}

func (p *Pipeline) addSource(src Node) {
	src.setPipeline(p)
	_ = p.assignID(src)
	p.sources = append(p.sources, src)
}

func (p *Pipeline) assignID(n Node) error {
	n.setID(p.id)
	p.id++
	return nil
}

// The number of nodes in the pipeline.
// tick:ignore
func (p *Pipeline) Len() int {
	if p.sorted == nil {
		p.sort()
	}
	return len(p.sorted)
}

// Walks the entire pipeline and calls func f on each node exactly once.
// f will be called on a node n only after all of its parents have already had f called.
// tick:ignore
func (p *Pipeline) Walk(f func(n Node) error) error {
	if p.sorted == nil {
		p.sort()
	}
	for _, n := range p.sorted {
		err := f(n)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Pipeline) sort() {
	// Iterate the sources in reverse order
	for i := len(p.sources) - 1; i >= 0; i-- {
		p.visit(p.sources[i])
	}
	//reverse p.sorted
	s := p.sorted
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

// Depth first search topological sorting of a DAG.
// https://en.wikipedia.org/wiki/Topological_sorting#Algorithms
func (p *Pipeline) visit(n Node) {
	if n.tMark() {
		panic("pipeline contains a cycle")
	}
	if !n.pMark() {
		n.setTMark(true)
		for _, c := range n.Children() {
			p.visit(c)
		}
		n.setPMark(true)
		n.setTMark(false)
		p.sorted = append(p.sorted, n)
	}
}

// Return a graphviz .dot formatted byte array.
// tick:ignore
func (p *Pipeline) Dot(name string) []byte {

	var buf bytes.Buffer

	buf.Write([]byte("digraph "))
	buf.Write([]byte(name))
	buf.Write([]byte(" {\n"))
	_ = p.Walk(func(n Node) error {
		n.dot(&buf)
		return nil
	})
	buf.Write([]byte("}"))

	return buf.Bytes()
}

//tick:ignore
type TemplatePipeline struct {
	p    *Pipeline
	vars map[string]tick.Var
}

// Return the set of vars defined by the TICKscript with their defaults
// tick:ignore
func (t *TemplatePipeline) Vars() map[string]tick.Var {
	return t.vars
}

// Return a graphviz .dot formatted byte array.
// tick:ignore
func (t *TemplatePipeline) Dot(name string) []byte {
	return t.p.Dot(name)
}
