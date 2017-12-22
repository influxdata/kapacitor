package tick

import (
	"sort"

	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// SideloadNode converts the Sideload pipeline node into the TICKScript AST
type SideloadNode struct {
	Function
}

// NewSideload creates a Sideload function builder
func NewSideload(parents []ast.Node) *SideloadNode {
	return &SideloadNode{
		Function{
			Parents: parents,
		},
	}
}

// Build creates a Sideload ast.Node
func (n *SideloadNode) Build(d *pipeline.SideloadNode) (ast.Node, error) {
	n.Pipe("sideload")

	n.Dot("source", d.Source)
	order := make([]interface{}, len(d.OrderList))
	for i := range d.OrderList {
		order[i] = d.OrderList[i]
	}
	n.Dot("order", order...)

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
	sort.Strings(tagKeys)
	for _, k := range tagKeys {
		n.Dot("tag", k, d.Tags[k])
	}
	return n.prev, n.err
}
