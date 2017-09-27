package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// Stats converts the stats pipeline node into the TICKScript AST
type Stats struct {
	Function
}

// Build stats ast.Node
func (n *Stats) Build(s *pipeline.StatsNode) (ast.Node, error) {
	n.Pipe("stats", s.Interval).
		DotIF("align", s.AlignFlag)
	return n.prev, n.err
}
