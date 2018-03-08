package tick

import (
	"fmt"

	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// AST converts a pipeline into an AST
type AST struct {
	Program   ast.ProgramNode
	parents   map[string]ast.Node
	statsSrcs []string
	err       error
}

// Build constructs the AST Program node from the pipeline
func (a *AST) Build(p *pipeline.Pipeline) error {
	a.parents = map[string]ast.Node{}
	a.statsSrcs = statSources(p.Stats())
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
	return a.err
}

// Link inspects the pipeline node to determine if it
// should become a variable, or, be considered "complete."
func (a *AST) Link(node pipeline.Node, function ast.Node) {
	// Special case where the stats node "parent" is this node if this node
	// is having stats collected.
	if a.haveStats(node) {
		a.Variable(node.Name(), function)
		return
	}

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
		return NewUnion(parents).Build(node)
	case *pipeline.JoinNode:
		return NewJoin(parents).Build(node)
	case *pipeline.AlertNode:
		return NewAlert(parents).Build(node)
	case *pipeline.BarrierNode:
		return NewBarrierNode(parents).Build(node)
	case *pipeline.CombineNode:
		return NewCombine(parents).Build(node)
	case *pipeline.DefaultNode:
		return NewDefault(parents).Build(node)
	case *pipeline.DeleteNode:
		return NewDelete(parents).Build(node)
	case *pipeline.DerivativeNode:
		return NewDerivative(parents).Build(node)
	case *pipeline.ChangeDetectNode:
		return NewChangeDetect(parents).Build(node)
	case *pipeline.Ec2AutoscaleNode:
		return NewEc2Autoscale(parents).Build(node)
	case *pipeline.EvalNode:
		return NewEval(parents).Build(node)
	case *pipeline.FlattenNode:
		return NewFlatten(parents).Build(node)
	case *pipeline.FromNode:
		return NewFrom(parents).Build(node)
	case *pipeline.GroupByNode:
		return NewGroupBy(parents).Build(node)
	case *pipeline.HTTPOutNode:
		return NewHTTPOut(parents).Build(node)
	case *pipeline.HTTPPostNode:
		return NewHTTPPost(parents).Build(node)
	case *pipeline.InfluxDBOutNode:
		return NewInfluxDBOut(parents).Build(node)
	case *pipeline.InfluxQLNode:
		return NewInfluxQL(parents).Build(node)
	case *pipeline.K8sAutoscaleNode:
		return NewK8sAutoscale(parents).Build(node)
	case *pipeline.KapacitorLoopbackNode:
		return NewKapacitorLoopbackNode(parents).Build(node)
	case *pipeline.LogNode:
		return NewLog(parents).Build(node)
	case *pipeline.QueryNode:
		return NewQuery(parents).Build(node)
	case *pipeline.SampleNode:
		return NewSample(parents).Build(node)
	case *pipeline.ShiftNode:
		return NewShift(parents).Build(node)
	case *pipeline.SideloadNode:
		return NewSideload(parents).Build(node)
	case *pipeline.StateCountNode:
		return NewStateCount(parents).Build(node)
	case *pipeline.StateDurationNode:
		return NewStateDuration(parents).Build(node)
	case *pipeline.SwarmAutoscaleNode:
		return NewSwarmAutoscale(parents).Build(node)
	case *pipeline.UDFNode:
		return NewUDF(parents).Build(node)
	case *pipeline.WhereNode:
		return NewWhere(parents).Build(node)
	case *pipeline.WindowNode:
		return NewWindowNode(parents).Build(node)
	case *pipeline.StreamNode:
		s := StreamNode{}
		return s.Build()
	case *pipeline.BatchNode:
		b := BatchNode{}
		return b.Build()
	case *pipeline.StatsNode:
		return NewStats(a.statParent(node)).Build(node)
	case *pipeline.NoOpNode: // NoOpNodes are swallowed
		return nil, nil
	default:
		return nil, fmt.Errorf("Unknown pipeline node %T", node)
	}
}

func (a *AST) parentsOf(n pipeline.Node) []ast.Node {
	p := make([]ast.Node, len(n.Parents()))
	for i, parent := range n.Parents() {
		p[i] = a.parents[parent.Name()]
	}
	return p
}

func (a *AST) haveStats(n pipeline.Node) bool {
	for i := range a.statsSrcs {
		if a.statsSrcs[i] == n.Name() {
			return true
		}
	}
	return false
}

func (a *AST) statParent(stat *pipeline.StatsNode) []ast.Node {
	name := stat.SourceNode.Name()
	return []ast.Node{a.parents[name]}
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

func statSources(stats []*pipeline.StatsNode) []string {
	srcs := make([]string, len(stats))
	for i, stat := range stats {
		srcs[i] = stat.SourceNode.Name()
	}
	return srcs
}
