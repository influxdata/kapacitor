package tick

import (
	"bytes"
	"log"

	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// AST converts a pipeline into an AST
type AST struct {
	Program ast.ProgramNode
	parents map[string]ast.Node
	err     error
}

// Build constructs the AST Program node from the pipeline
func (a *AST) Build(p *pipeline.Pipeline) {
	// 1. If there is more than one child then node is a variable with name of node
	// 2. If the child's first parent is not this node, then this should be a variable.
	// 3. If there are no children, then, this node is added to the program
	// 4. If there are more than one parent then you can assume the parents are variables
	p.Walk(func(node pipeline.Node) error {
		if a.err != nil {
			return a.err
		}

		parents := a.parentsOf(node)
		function, err := a.Create(node, parents)
		if err != nil {
			a.err = err
			return err
		}

		// If the function has meaning, do not attach to tree
		if function == nil {
			return nil
		}

		a.Link(node, function)
		return nil
	})
}

// Link inspects the pipeline node to determine if it
// should become a variable, or, be considered "complete."
func (a *AST) Link(node pipeline.Node, function ast.Node) {
	children := node.Children()
	switch len(children) {
	case 0:
		// When there are no more children, this function is complete and
		// is should be added to the program.
		a.Program.Add(function)
	case 1:
		// If this node is not the left-most parent of its child, then we need
		// it to be a variable by falling through to the default case.
		parent := children[0].Parents()[0]
		if parent.ID() == node.ID() {
			a.parents[node.Name()] = function
			return
		}
		fallthrough
	default:
		// If there is more than one child then we know
		// this function will be used as a variable
		// during a later node visit.
		a.Variable(node.Name(), function)
	}
}

// Create converts a pipeline Node to a function
func (a *AST) Create(n pipeline.Node, parents []ast.Node) (ast.Node, error) {
	switch node := n.(type) {
	case *pipeline.UnionNode:
		return Union{Parents: parents}.Build(node)
	case *pipeline.JoinNode:
		return Join{Parents: parents}.Build(node)
	case *pipeline.AlertNode:
		return Alert{Parents: parents}.Build(node)
	case *pipeline.CombineNode:
		return Combine{Parents: parents}.Build(node)
	case *pipeline.DefaultNode:
		return Default{Parents: parents}.Build(node)
	case *pipeline.DeleteNode:
		return Delete{Parents: parents}.Build(node)
	case *pipeline.DerivativeNode:
		return Derivative{Parents: parents}.Build(node)
	case *pipeline.EvalNode:
		return Eval{Parents: parents}.Build(node)
	case *pipeline.FlattenNode:
		return Flatten{Parents: parents}.Build(node)
	case *pipeline.FlattenNode:
		return Flatten{Parents: parents}.Build(node)
	case *pipeline.FromNode:
		return From{Parents: parents}.Build(node)
	case *pipeline.GroupByNode:
		return GroupBy{Parents: parents}.Build(node)
	case *pipeline.HTTPOutNode:
		return HTTPOut{Parents: parents}.Build(node)
	case *pipeline.HTTPPostNode:
		return HTTPPost{Parents: parents}.Build(node)
	case *pipeline.InfluxDBOutNode:
		return InfluxDBOut{Parents: parents}.Build(node)
	case *pipeline.InfluxQLNode:
		return InfluxQL{Parents: parents}.Build(node)
	case *pipeline.K8sAutoscaleNode:
		return K8sAutoscale{Parents: parents}.Build(node)
	case *pipeline.LogNode:
		return Log{Parents: parents}.Build(node)
	case *pipeline.QueryNode:
		return Query{Parents: parents}.Build(node)
	case *pipeline.SampleNode:
		return Sample{Parents: parents}.Build(node)
	case *pipeline.ShiftNode:
		return Shift{Parents: parents}.Build(node)
	case *pipeline.StateCountNode:
		return StateCount{Parents: parents}.Build(node)
	case *pipeline.StateDurationNode:
		return StateDuration{Parents: parents}.Build(node)
	case *pipeline.SwarmAutoscaleNode:
		return SwarmAutoscale{Parents: parents}.Build(node)
	case *pipeline.UDFNode:
		return UDF{Parents: parents}.Build(node)
	case *pipeline.Where:
		return Where{Parents: parents}.Build(node)
	case *pipeline.Window:
		return Window{Parents: parents}.Build(node)
	case *pipeline.StreamNode:
		return Stream{}.Build()
	case *pipeline.BatchNode:
		return Batch{}.Build()
	case *pipeline.StatsNode:
		return Stats{Parents: parents}.Build(node)
	}
}

// TICKScript produces a TICKScript from the AST
func (a *AST) TICKScript() string {
	var buf bytes.Buffer
	log.Printf("%#+v", a.prev)
	a.prev.Format(&buf, "", false)
	return buf.String()
}

func (a *AST) parentsOf(n pipeline.Node) []ast.Node {
	p := make([]ast.Node, len(n.Parents()))
	for i, parent := range n.Parents() {
		p[i] = a.parents[parent.Name()]
	}
	return p
}

// Variable produces an ast.DeclarationNode using ident as the
// identifier name.
func (a *AST) Variable(ident string, right ast.Node) *AST {
	if a.err != nil {
		return a
	}

	id := &ast.IdentifierNode{
		Ident: ident,
	}

	a.Program.Add(&ast.DeclarationNode{
		Left:  id,
		Right: right,
	})

	// Vars used to allow children to lookup parents.
	a.parents[ident] = id
	return a
}
