package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// Eval converts the Eval pipeline node into the TICKScript AST
type Eval struct {
	Function
}

// Build creates a Eval ast.Node
func (n *Eval) Build(e *pipeline.EvalNode) (ast.Node, error) {
	n.Pipe("eval", e.Lambdas...).
		Dot("as", e.AsList...).
		Dot("tags", e.TagList...).
		DotIf("quiet", e.QuietFlag)

	if e.KeepFlag {
		n.Dot("keep", e.KeepList...)
	}

	return n.prev, n.err
}
