package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// SampleNode converts the SampleNode pipeline node into the TICKScript AST
type SampleNode struct {
	Function
}

// NewSample creates a SampleNode function builder
func NewSample(parents []ast.Node) *SampleNode {
	return &SampleNode{
		Function{
			Parents: parents,
		},
	}
}

// Build creates a SampleNode ast.Node
func (n *SampleNode) Build(s *pipeline.SampleNode) (ast.Node, error) {
	n.Pipe("sample", s.N, s.Duration)
	return n.prev, n.err
}
