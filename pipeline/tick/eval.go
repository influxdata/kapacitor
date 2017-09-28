package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// Eval converts the Eval pipeline node into the TICKScript AST
type Eval struct {
	Function
}

// NewEval creates a Eval function builder
func NewEval(parents []ast.Node) *Eval {
	return &Eval{
		Function{
			Parents: parents,
		},
	}
}

// Build creates a Eval ast.Node
func (n *Eval) Build(e *pipeline.EvalNode) (ast.Node, error) {
	n.Pipe("eval", largs(e.Lambdas)...).
		Dot("as", args(e.AsList)...).
		Dot("tags", args(e.TagsList)...).
		DotIf("quiet", e.QuietFlag)

	if e.KeepFlag {
		n.Dot("keep", args(e.KeepList)...)
	}

	return n.prev, n.err
}
