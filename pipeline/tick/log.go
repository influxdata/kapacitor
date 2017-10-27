package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// LogNode converts the LogNode pipeline node into the TICKScript AST
type LogNode struct {
	Function
}

// NewLog creates a LogNode function builder
func NewLog(parents []ast.Node) *LogNode {
	return &LogNode{
		Function{
			Parents: parents,
		},
	}
}

// Build creates a LogNode ast.Node
func (n *LogNode) Build(l *pipeline.LogNode) (ast.Node, error) {
	n.Pipe("log").
		Dot("level", l.Level).
		Dot("prefix", l.Prefix)

	return n.prev, n.err
}
