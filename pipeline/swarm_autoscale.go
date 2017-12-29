package pipeline

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/kapacitor/tick/ast"
)

// SwarmAutoscaleNode triggers autoscale events for a service on a Docker Swarm mode cluster.
// The node also outputs points for the triggered events.
//
// Example:
//     // Target 80% cpu per container
//     var target = 80.0
//     var min = 1
//     var max = 10
//     var period = 5m
//     var every = period
//     stream
//         |from()
//             .measurement('docker_container_cpu')
//             .groupBy('container_name','com.docker.swarm.service.name')
//             .where(lambda: "cpu" == 'cpu-total')
//         |window()
//             .period(period)
//             .every(every)
//         |mean('usage_percent')
//             .as('mean_cpu')
//         |groupBy('com.docker.swarm.service.name')
//         |sum('mean_cpu')
//             .as('total_cpu')
//         |swarmAutoscale()
//             // Get the name of the service from "com.docker.swarm.service.name" tag.
//             .serviceNameTag('com.docker.swarm.service.name')
//             .min(min)
//             .max(max)
//             // Set the desired number of replicas based on target.
//             .replicas(lambda: int(ceil("total_cpu" / target)))
//         |influxDBOut()
//             .database('deployments')
//             .measurement('scale_events')
//             .precision('s')
//
//
// The above example computes the mean of cpu usage_percent by container name and service name.
// Then sum of mean cpu_usage is calculated as total_cpu.
// Using the total_cpu over the last time period a desired number of replicas is computed
// based on the target percentage usage of cpu.
//
// If the desired number of replicas has changed, Kapacitor makes the appropriate API call to Docker Swarm
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
//    * errors          -- number of errors encountered, typically related to communicating with the Swarm manager API.
//
type SwarmAutoscaleNode struct {
	chainnode `json:"-"`

	// Cluster is the ID docker swarm cluster to use.
	// The ID of the cluster is specified in the kapacitor configuration.
	Cluster string `json:"cluster"`

	// ServiceName is the name of the docker swarm service to autoscale.
	ServiceName string `json:"serviceName"`
	// ServiceName is the name of a tag which contains the name of the docker swarm service to autoscale.
	ServiceNameTag string `json:"serviceNameTag"`
	// OutputServiceName is the name of a tag into which the service name will be written for output autoscale events.
	// Defaults to the value of ServiceNameTag if its not empty.
	OutputServiceNameTag string `json:"outputServiceNameTag"`

	// CurrentField is the name of a field into which the current replica count will be set as an int.
	// If empty no field will be set.
	// Useful for computing deltas on the current state.
	//
	// Example:
	//    |swarmAutoscale()
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
}

func newSwarmAutoscaleNode(e EdgeType) *SwarmAutoscaleNode {
	k := &SwarmAutoscaleNode{
		chainnode: newBasicChainNode("swarm_autoscale", e, StreamEdge),
		Min:       1,
	}
	return k
}

func (n *SwarmAutoscaleNode) validate() error {

	if (n.ServiceName == "" && n.ServiceNameTag == "") ||
		(n.ServiceName != "" && n.ServiceNameTag != "") {
		return fmt.Errorf("must specify exactly one of ServiceName or ServiceNameTag")
	}
	if n.Min < 1 {
		return fmt.Errorf("min must be >= 1, got %d", n.Min)
	}
	if n.Replicas == nil {
		return errors.New("must provide a replicas lambda expression")
	}
	return nil
}

// MarshalJSON converts SwarmAutoscaleNode to JSON
// tick:ignore
func (n *SwarmAutoscaleNode) MarshalJSON() ([]byte, error) {
	type Alias SwarmAutoscaleNode
	var raw = &struct {
		TypeOf
		*Alias
		IncreaseCooldown string `json:"increaseCooldown"`
		DecreaseCooldown string `json:"decreaseCooldown"`
	}{
		TypeOf: TypeOf{
			Type: "swarmAutoscale",
			ID:   n.ID(),
		},
		Alias:            (*Alias)(n),
		IncreaseCooldown: influxql.FormatDuration(n.IncreaseCooldown),
		DecreaseCooldown: influxql.FormatDuration(n.DecreaseCooldown),
	}
	return json.Marshal(raw)
}

// UnmarshalJSON converts JSON to an SwarmAutoscaleNode
// tick:ignore
func (n *SwarmAutoscaleNode) UnmarshalJSON(data []byte) error {
	type Alias SwarmAutoscaleNode
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

	if raw.Type != "swarmAutoscale" {
		return fmt.Errorf("error unmarshaling node %d of type %s as SwarmAutoscaleNode", raw.ID, raw.Type)
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
