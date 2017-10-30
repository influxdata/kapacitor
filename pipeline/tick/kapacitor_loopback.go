package tick

import (
	"sort"

	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// KapacitorLoopbackNode converts the KapacitorLoopbackNode pipeline node into the TICKScript AST
type KapacitorLoopbackNode struct {
	Function
}

// NewKapacitorLoopbackNode creates a KapacitorLoopbackNode function builder
func NewKapacitorLoopbackNode(parents []ast.Node) *KapacitorLoopbackNode {
	return &KapacitorLoopbackNode{
		Function{
			Parents: parents,
		},
	}
}

// Build creates a KapacitorLoopbackNode ast.Node
func (n *KapacitorLoopbackNode) Build(k *pipeline.KapacitorLoopbackNode) (ast.Node, error) {
	n.Pipe("kapacitorLoopback").
		Dot("database", k.Database).
		Dot("retentionPolicy", k.RetentionPolicy).
		Dot("measurement", k.Measurement)

	var tagKeys []string
	for key := range k.Tags {
		tagKeys = append(tagKeys, key)
	}
	sort.Strings(tagKeys)
	for _, key := range tagKeys {
		n.Dot("tag", key, k.Tags[key])
	}

	return n.prev, n.err
}
