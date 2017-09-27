package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// K8sAutoscale converts the k8s autoscaling pipeline node into the TICKScript AST
type K8sAutoscale struct {
	Function
}

// Build creates a K8sAutoscale ast.Node
func (n *K8sAutoscale) Build(k *pipeline.K8sAutoscaleNode) (ast.Node, error) {
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
