package pipeline

import (
	"fmt"
	"time"

	"github.com/influxdata/kapacitor/services/k8s/client"
	"github.com/influxdata/kapacitor/tick/ast"
)

// K8sAutoscaleNode triggers autoscale events for a resource on a kubernetes cluster.
// The node also outputs points for the triggered events.
//
// Example:
//    var increase_threshold = 1000
//    var decrease_threshold = 500
//    stream
//        |from()
//            .measurement('requests')
//            .groupBy('service', 'host')
//        |derivative('count')
//            .as('request_per_second')
//            .unit(1s)
//            .nonNegative()
//        |groupBy('service')
//        |window()
//            .period(1m)
//            .every(1m)
//        |percentile('request_per_second', 95.0)
//        |k8sAutoscale()
//            .kind('deployments')
//            .nameTag('service')
//            .min(10)
//            .max(100)
//            .increase(lambda: if("percentile" > increase_threshold, 1, 0))
//            .decrease(lambda: if("percentile" < decrease_threshold, 1, 0))
//
// The above example computes the requests per second by server and host.
// Then the 95th percentile over the last minute across all hosts for each service is computed.
// If the 95th percentile is above the increase_threshold then the scale factor is increased by 1.
// If the 95th percentile is below the decrease_threshold then the scale factor is decreased by 1.
//
// Notice that a clear maximum rate of change is evident because only 1 instance can be added or removed per minute.
// In addition to constraining the rate via the pipeline, explicit cooldowns can be set to limit the number of scale events triggered.
type K8sAutoscaleNode struct {
	chainnode

	// Kind is the type of resources to autoscale.
	// Currently only "deployments" and "replicationcontrollers" are supported.
	// Default: "deployments"
	Kind string

	// ResourceName is the name of the resource to autoscale.
	ResourceName string

	// ResourceNameField is the name of a field that names the resource to autoscale.
	ResourceNameField string

	// ResourceNameTag is the name of a tag that names the resource to autoscale.
	ResourceNameTag string

	// Namespace is the namespace of the resource, if empty the default namespace will be used.
	Namespace string

	// CurrentField is the name of a field into which the current scale factor will be set as an int.
	// If empty no field will be set.
	CurrentField string

	// The maximum scale factor to set.
	// If 0 then there is no upper limit.
	// Default: 0, a.k.a no limit.
	Max int64

	// The minimum scale factor to set.
	// Default: 1
	Min int64

	// An expression which if it evaluates to a positive value, will increase the desired
	// scale factor by the returned amount, up to K8sAutoscaleNode.Max
	Increase *ast.LambdaNode
	// An expression which if it evaluates to a positive value, will decrease the desired
	// scale factor by the returned amount, down to K8sAutoscaleNode.Min
	Decrease *ast.LambdaNode
	// An expression which if it evaluates to a value within the (min,max) range, will set the desired
	// scale factor to the returned amount.
	Set *ast.LambdaNode

	// Only one increase event can be triggered per resource every IncreaseCooldown
	IncreaseCooldown time.Duration
	// Only one decrease event can be triggered per resource every DecreaseCooldown
	DecreaseCooldown time.Duration
}

func newK8sAutoscaleNode(e EdgeType) *K8sAutoscaleNode {
	k := &K8sAutoscaleNode{
		chainnode: newBasicChainNode("k8s_autoscale", e, StreamEdge),
		Min:       1,
		Kind:      client.DeploymentsKind,
	}
	return k
}

func (n *K8sAutoscaleNode) validate() error {
	if n.ResourceName != "" && (n.ResourceNameField != "" || n.ResourceNameTag != "") {
		return fmt.Errorf("must specify exactly one of ResourceName, ResourceNameField and ResourceNameTag")
	}
	if n.ResourceNameField != "" && (n.ResourceName != "" || n.ResourceNameTag != "") {
		return fmt.Errorf("must specify exactly one of ResourceName, ResourceNameField and ResourceNameTag")
	}
	if n.ResourceNameTag != "" && (n.ResourceName != "" || n.ResourceNameField != "") {
		return fmt.Errorf("must specify exactly one of ResourceName, ResourceNameField and ResourceNameTag")
	}
	if n.ResourceNameTag == "" && n.ResourceName == "" && n.ResourceNameField == "" {
		return fmt.Errorf("must specify exactly one of ResourceName, ResourceNameField and ResourceNameTag")
	}
	if n.Kind != client.DeploymentsKind && n.Kind != client.ReplicationControllerKind {
		return fmt.Errorf("invalid Kind, must be 'deployments' or 'replicationcontrollers', got %s", n.Kind)
	}
	if n.Min < 1 {
		return fmt.Errorf("min must be >= 1, got %d", n.Min)
	}
	return nil
}
