package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// K8sAutoscaleNode converts the K8sAutoscaleNode pipeline node into the TICKScript AST
type K8sAutoscaleNode struct {
	Function
}

// NewK8sAutoscale creates a K8sAutoscaleNode function builder
func NewK8sAutoscale(parents []ast.Node) *K8sAutoscaleNode {
	return &K8sAutoscaleNode{
		Function{
			Parents: parents,
		},
	}
}

// Build creates a K8sAutoscaleNode ast.Node
func (n *K8sAutoscaleNode) Build(k *pipeline.K8sAutoscaleNode) (ast.Node, error) {
	n.Pipe("k8sAutoscale").
		Dot("cluster", k.Cluster).
		Dot("namespace", k.Namespace).
		Dot("kind", k.Kind).
		Dot("resourceName", k.ResourceName).
		Dot("resourceNameTag", k.ResourceNameTag).
		Dot("currentField", k.CurrentField).
		Dot("max", k.Max).
		Dot("min", k.Min).
		Dot("replicas", k.Replicas).
		Dot("increaseCooldown", k.IncreaseCooldown).
		Dot("decreaseCooldown", k.DecreaseCooldown).
		Dot("namespaceTag", k.NamespaceTag).
		Dot("kindTag", k.KindTag).
		Dot("resourceTag", k.ResourceTag)

	return n.prev, n.err
}
