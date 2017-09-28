package tick

import (
	"sort"

	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// Default converts the Default pipeline node into the TICKScript AST
type Default struct {
	Function
}

// NewDefault creates a Default function builder
func NewDefault(parents []ast.Node) *Default {
	return &Default{
		Function{
			Parents: parents,
		},
	}
}

// Build creates a Default ast.Node
func (n *Default) Build(d *pipeline.DefaultNode) (ast.Node, error) {
	n.Pipe("default")
	var fieldKeys []string
	for k := range d.Fields {
		fieldKeys = append(fieldKeys, k)
	}
	sort.Strings(fieldKeys)
	for _, k := range fieldKeys {
		n.Dot("field", k, d.Fields[k])
	}

	var tagKeys []string
	for k := range d.Tags {
		tagKeys = append(tagKeys, k)
	}
	for _, k := range tagKeys {
		n.Dot("tag", k, d.Tags[k])
	}
	return n.prev, n.err
}
