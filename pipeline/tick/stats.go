package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// StatsNode converts the StatsNode pipeline node into the TICKScript AST
type StatsNode struct {
	Function
}

// NewStats creates a StatsNode function builder
func NewStats(parents []ast.Node) *StatsNode {
	return &StatsNode{
		Function{
			Parents: parents,
		},
	}
}

// Build StatsNode ast.Node
func (n *StatsNode) Build(s *pipeline.StatsNode) (ast.Node, error) {
	n.Pipe("stats", s.Interval).
		DotIf("align", s.AlignFlag)
	return n.prev, n.err
}
