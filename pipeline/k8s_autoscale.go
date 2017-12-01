package pipeline

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/kapacitor/services/k8s/client"
	"github.com/influxdata/kapacitor/tick/ast"
)

const (
	DefaultNamespaceTag = "namespace"
	DefaultKindTag      = "kind"
	DefaultResourceTag  = "resource"
)

// K8sAutoscaleNode triggers autoscale events for a resource on a Kubernetes cluster.
// The node also outputs points for the triggered events.
//
// Example:
//     // Target 100 requests per second per host
//     var target = 100.0
//     var min = 1
//     var max = 100
//     var period = 5m
//     var every = period
//     stream
//         |from()
//             .measurement('requests')
//             .groupBy('host', 'deployment')
//             .truncate(1s)
//         |derivative('value')
//             .as('requests_per_second')
//             .unit(1s)
//             .nonNegative()
//         |groupBy('deployment')
//         |sum('requests_per_second')
//             .as('total_requests')
//         |window()
//             .period(period)
//             .every(every)
//         |mean('total_requests')
//             .as('total_requests')
//         |k8sAutoscale()
//             // Get the name of the deployment from the 'deployment' tag.
//             .resourceNameTag('deployment')
//             .min(min)
//             .max(max)
//             // Set the desired number of replicas based on target.
//             .replicas(lambda: int(ceil("total_requests" / target)))
//         |influxDBOut()
//             .database('deployments')
//             .measurement('scale_events')
//             .precision('s')
//
//
// The above example computes the requests per second by deployment and host.
// Then the total_requests per second across all hosts is computed per deployment.
// Using the mean of the total_requests over the last time period a desired number of replicas is computed
// based on the target number of request per second per host.
//
// If the desired number of replicas has changed, Kapacitor makes the appropriate API call to Kubernetes
// to update the replicas spec.
//
// Any time the k8sAutoscale node changes a replica count, it emits a point.
// The point is tagged with the namespace, kind and resource name,
// using the NamespaceTag, KindTag, and ResourceTag properties respectively.
// In addition the group by tags will be preserved on the emitted point.
// The point contains two fields: `old`, and `new` representing change in the replicas.
//
// Available Statistics:
//
//    * increase_events -- number of times the replica count was increased.
//    * decrease_events -- number of times the replica count was decreased.
//    * cooldown_drops  -- number of times an event was dropped because of a cooldown timer.
//    * errors          -- number of errors encountered, typically related to communicating with the Kubernetes API.
//
type K8sAutoscaleNode struct {
	chainnode `json:"-"`

	// Cluster is the name of the Kubernetes cluster to use.
	Cluster string `json:"cluster"`

	// Namespace is the namespace of the resource, if empty the default namespace will be used.
	Namespace string `json:"namespace"`

	// Kind is the type of resources to autoscale.
	// Currently only "deployments", "replicasets" and "replicationcontrollers" are supported.
	// Default: "deployments"
	Kind string `json:"kind"`

	// ResourceName is the name of the resource to autoscale.
	ResourceName string `json:"resourceName"`

	// ResourceNameTag is the name of a tag that names the resource to autoscale.
	ResourceNameTag string `json:"resourceNameTag"`

	// CurrentField is the name of a field into which the current replica count will be set as an int.
	// If empty no field will be set.
	// Useful for computing deltas on the current state.
	//
	// Example:
	//    |k8sAutoscale()
	//        .currentField('replicas')
	//        // Increase the replicas by 1 if the qps is over the threshold
	//        .replicas(lambda: if("qps" > threshold, "replicas" + 1, "replicas"))
	//
	CurrentField string `json:"currentField"`

	// The maximum scale factor to set.
	// If 0 then there is no upper limit.
	// Default: 0, a.k.a no limit.
	Max int64 `json:"max"`

	// The minimum scale factor to set.
	// Default: 1
	Min int64 `json:"min"`

	// Replicas is a lambda expression that should evaluate to the desired number of replicas for the resource.
	Replicas *ast.LambdaNode `json:"replicas"`

	// Only one increase event can be triggered per resource every IncreaseCooldown interval.
	IncreaseCooldown time.Duration `json:"increaseCooldown"`
	// Only one decrease event can be triggered per resource every DecreaseCooldown interval.
	DecreaseCooldown time.Duration `json:"decreaseCooldown"`

	// NamespaceTag is the name of a tag to use when tagging emitted points with the namespace.
	// If empty the point will not be tagged with the resource.
	// Default: namespace
	NamespaceTag string `json:"namespaceTag"`

	// KindTag is the name of a tag to use when tagging emitted points with the kind.
	// If empty the point will not be tagged with the resource.
	// Default: kind
	KindTag string `json:"kindTag"`

	// ResourceTag is the name of a tag to use when tagging emitted points the resource.
	// If empty the point will not be tagged with the resource.
	// Default: resource
	ResourceTag string `json:"resourceTag"`
}

func newK8sAutoscaleNode(e EdgeType) *K8sAutoscaleNode {
	k := &K8sAutoscaleNode{
		chainnode:    newBasicChainNode("k8s_autoscale", e, StreamEdge),
		Min:          1,
		Kind:         client.DeploymentsKind,
		NamespaceTag: DefaultNamespaceTag,
		KindTag:      DefaultKindTag,
		ResourceTag:  DefaultResourceTag,
	}
	return k
}

// MarshalJSON converts K8sAutoscaleNode to JSON
// tick:ignore
func (n *K8sAutoscaleNode) MarshalJSON() ([]byte, error) {
	type Alias K8sAutoscaleNode
	var raw = &struct {
		TypeOf
		*Alias
		IncreaseCooldown string `json:"increaseCooldown"`
		DecreaseCooldown string `json:"decreaseCooldown"`
	}{
		TypeOf: TypeOf{
			Type: "k8sAutoscale",
			ID:   n.ID(),
		},
		Alias:            (*Alias)(n),
		IncreaseCooldown: influxql.FormatDuration(n.IncreaseCooldown),
		DecreaseCooldown: influxql.FormatDuration(n.DecreaseCooldown),
	}
	return json.Marshal(raw)
}

// UnmarshalJSON converts JSON to an K8sAutoscaleNode
// tick:ignore
func (n *K8sAutoscaleNode) UnmarshalJSON(data []byte) error {
	type Alias K8sAutoscaleNode
	var raw = &struct {
		TypeOf
		*Alias
		IncreaseCooldown string `json:"increaseCooldown"`
		DecreaseCooldown string `json:"decreaseCooldown"`
	}{
		Alias: (*Alias)(n),
	}
	err := json.Unmarshal(data, raw)
	if err != nil {
		return err
	}

	if raw.Type != "k8sAutoscale" {
		return fmt.Errorf("error unmarshaling node %d of type %s as K8sAutoscaleNode", raw.ID, raw.Type)
	}

	n.IncreaseCooldown, err = influxql.ParseDuration(raw.IncreaseCooldown)
	if err != nil {
		return err
	}

	n.DecreaseCooldown, err = influxql.ParseDuration(raw.DecreaseCooldown)
	if err != nil {
		return err
	}

	n.setID(raw.ID)
	return nil
}

func (n *K8sAutoscaleNode) validate() error {
	if (n.ResourceName != "" && n.ResourceNameTag != "") ||
		(n.ResourceNameTag == "" && n.ResourceName == "") {
		return fmt.Errorf("must specify exactly one of ResourceName or ResourceNameTag")
	}
	if n.Kind != client.DeploymentsKind && n.Kind != client.ReplicationControllerKind && n.Kind != client.ReplicaSetsKind {
		return fmt.Errorf("invalid Kind, must be 'deployments', 'replicasets' or 'replicationcontrollers', got %s", n.Kind)
	}
	if n.Min < 1 {
		return fmt.Errorf("min must be >= 1, got %d", n.Min)
	}
	if n.Replicas == nil {
		return errors.New("must provide a replicas lambda expression")
	}
	return nil
}
