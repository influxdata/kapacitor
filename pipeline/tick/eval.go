package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// EvalNode converts the EvalNode pipeline node into the TICKScript AST
type EvalNode struct {
	Function
}

// NewEval creates a EvalNode function builder
func NewEval(parents []ast.Node) *EvalNode {
	return &EvalNode{
		Function{
			Parents: parents,
		},
	}
}

// Build creates a Eval ast.Node
func (n *EvalNode) Build(e *pipeline.EvalNode) (ast.Node, error) {
	n.Pipe("eval", largs(e.Lambdas)...).
		Dot("as", args(e.AsList)...).
		Dot("tags", args(e.TagsList)...).
		DotIf("quiet", e.QuietFlag)

	if e.KeepFlag {
		n.Dot("keep", args(e.KeepList)...)
	}

	return n.prev, n.err
}
