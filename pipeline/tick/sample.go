package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// Sample converts the sample pipeline node into the TICKScript AST
type Sample struct {
	Function
}

// Build creates a Sample ast.Node
func (n *Sample) Build(s *pipeline.SampleNode) (ast.Node, error) {
	n.Pipe("sample", s.N, s.Duration)
	return n.prev, n.err
}
