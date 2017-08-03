package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// Ec2AutoscaleNode converts the ec2 autoscaling pipeline node into the TICKScript AST
type Ec2AutoscaleNode struct {
	Function
}

// NewEc2Autoscale creates a Ec2Autoscale function builder
func NewEc2Autoscale(parents []ast.Node) *Ec2AutoscaleNode {
	return &Ec2AutoscaleNode{
		Function{
			Parents: parents,
		},
	}
}

// Build creates a Ec2Autoscale ast.Node
func (n *Ec2AutoscaleNode) Build(s *pipeline.Ec2AutoscaleNode) (ast.Node, error) {
	n.Pipe("ec2Autoscale").
		Dot("cluster", s.Cluster).
		Dot("groupName", s.GroupNameTag).
		Dot("groupNameTag", s.GroupNameTag).
		Dot("outputGroupNameTag", s.OutputGroupNameTag).
		Dot("currentField", s.CurrentField).
		Dot("max", s.Max).
		Dot("min", s.Min).
		Dot("replicas", s.Replicas).
		Dot("increaseCooldown", s.IncreaseCooldown).
		Dot("decreaseCooldown", s.DecreaseCooldown)

	return n.prev, n.err
}
