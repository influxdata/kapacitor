package pipeline

import (
	"errors"
	"fmt"
	"time"

	"github.com/influxdata/kapacitor/tick/ast"
)

// SwarmAutoscaleNode triggers autoscale events for a service on a Docker Swarm mode cluster.
// The node also outputs points for the triggered events.
//
// Example:
//     // Target 80% cpu per container
//     var target = 80
//     var min = 1
//     var max = 10
//     var period = 5m
//     var every = period
//     stream
//         |from()
//             .measurement('docker_container_cpu')
//             .groupBy('com.docker.swarm.service.name')
//             .where(lambda: "cpu" == 'cpu-total')
//         |sum('usage_percent')
//             .as('total_cpu')
//         |window()
//             .period(period)
//             .every(every)
//         |mean('total_cpu')
//             .as('cpu_stat')
//         |SwarmAutoscale()
//             // Get the name of the deployment from the 'deployment' tag.
//             .serviceName('com.docker.swarm.service.name')
//             .min(min)
//             .max(max)
//             // Set the desired number of replicas based on target.
//             .replicas(lambda: int(ceil("cpu_stat" / target)))
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
// Any time the SwarmAutoscale node changes a replica count, it emits a point.
// The point is tagged with the service name,
// using the serviceName respectively
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
type SwarmAutoscaleNode struct {
	chainnode
	// ResourceName is the name of the resource to autoscale.
	ServiceName string

	// CurrentField is the name of a field into which the current replica count will be set as an int.
	// If empty no field will be set.
	// Useful for computing deltas on the current state.
	//
	// Example:
	//    |SwarmAutoscale()
	//        .currentField('replicas')
	//        // Increase the replicas by 1 if the qps is over the threshold
	//        .replicas(lambda: if("qps" > threshold, "replicas" + 1, "replicas"))
	//
	CurrentField string

	// The maximum scale factor to set.
	// If 0 then there is no upper limit.
	// Default: 0, a.k.a no limit.
	Max int64

	// The minimum scale factor to set.
	// Default: 1
	Min int64

	// Replicas is a lambda expression that should evaluate to the desired number of replicas for the resource.
	Replicas *ast.LambdaNode

	// Only one increase event can be triggered per resource every IncreaseCooldown interval.
	IncreaseCooldown time.Duration
	// Only one decrease event can be triggered per resource every DecreaseCooldown interval.
	DecreaseCooldown time.Duration
}

func newSwarmAutoscaleNode(e EdgeType) *SwarmAutoscaleNode {
	k := &SwarmAutoscaleNode{
		chainnode: newBasicChainNode("swarm_autoscale", e, StreamEdge),
		Min:       1,
	}
	return k
}

func (n *SwarmAutoscaleNode) validate() error {
	if n.ServiceName == "" {
		return fmt.Errorf("must specify exactly one of SwarmName")
	}
	if n.Min < 1 {
		return fmt.Errorf("min must be >= 1, got %d", n.Min)
	}
	if n.Replicas == nil {
		return errors.New("must provide a replicas lambda expression")
	}
	return nil
}
