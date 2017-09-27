package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// Stats converts the stats pipeline node into the TICKScript AST
type Stats struct {
	Function
}

// NewStats creates a Stats function builder
func NewStats(parents []ast.Node) *Stats {
	return &Stats{
		Function{
			Parents: parents,
		},
	}
}

// Build stats ast.Node
func (n *Stats) Build(s *pipeline.StatsNode) (ast.Node, error) {
	n.Pipe("stats", s.Interval).
		DotIf("align", s.AlignFlag)
	return n.prev, n.err
}
