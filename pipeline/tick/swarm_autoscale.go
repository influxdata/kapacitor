package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// SwarmAutoscale converts the swarm autoscaling pipeline node into the TICKScript AST
type SwarmAutoscale struct {
	Function
}

// NewSwarmAutoscale creates a SwarmAutoscale function builder
func NewSwarmAutoscale(parents []ast.Node) *SwarmAutoscale {
	return &SwarmAutoscale{
		Function{
			Parents: parents,
		},
	}
}

// Build creates a SwarmAutoscale ast.Node
func (n *SwarmAutoscale) Build(s *pipeline.SwarmAutoscaleNode) (ast.Node, error) {
	n.Pipe("swarmAutoscale").
		Dot("cluster", s.Cluster).
		Dot("servceName", s.ServiceNameTag).
		Dot("serviceNameTag", s.ServiceNameTag).
		Dot("outputServiceNameTag", s.OutputServiceNameTag).
		Dot("currentField", s.CurrentField).
		Dot("max", s.Max).
		Dot("min", s.Min).
		Dot("replicas", s.Replicas).
		Dot("increaseCooldown", s.IncreaseCooldown).
		Dot("decreaseCooldown", s.DecreaseCooldown)

	return n.prev, n.err
}
