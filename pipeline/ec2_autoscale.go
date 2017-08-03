package pipeline

import (
	"errors"
	"fmt"
	"time"

	"github.com/influxdata/kapacitor/tick/ast"
)

// EC2AutoscaleNode triggers autoscale events for a group on a AWS Autoscaling group.
// The node also outputs points for the triggered events.
//
// Example:
//     // Target 80% cpu per ec2 instance
//     var target = 80.0
//     var min = 1
//     var max = 10
//     var period = 5m
//     var every = period
//     stream
//         |from()
//             .measurement('cpu')
//             .groupBy('host_name','group_name')
//             .where(lambda: "cpu" == 'cpu-total')
//         |eval(lambda: 100.0 - "usage_idle")
//             .as('usage_percent')
//         |window()
//             .period(period)
//             .every(every)
//         |mean('usage_percent')
//             .as('mean_cpu')
//         |groupBy('group_name')
//         |sum('mean_cpu')
//             .as('total_cpu')
//         |ec2Autoscale()
//             // Get the group name of the VM(EC2 instance) from "group_name" tag.
//             .groupNameTag('group_name')
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
// The above example computes the mean of cpu usage_percent by host_name name and group_name
// Then sum of mean cpu_usage is calculated as total_cpu.
// Using the total_cpu over the last time period a desired number of replicas is computed
// based on the target percentage usage of cpu.
//
// If the desired number of replicas has changed, Kapacitor makes the appropriate API call to AWS autoscaling group
// to update the replicas spec.
//
// Any time the Ec2Autoscale node changes a replica count, it emits a point.
// The point is tagged with the group name,
// using the groupName respectively
// In addition the group by tags will be preserved on the emitted point.
// The point contains two fields: `old`, and `new` representing change in the replicas.
//
// Available Statistics:
//
//    * increase_events -- number of times the replica count was increased.
//    * decrease_events -- number of times the replica count was decreased.
//    * cooldown_drops  -- number of times an event was dropped because of a cooldown timer.
//    * errors          -- number of errors encountered, typically related to communicating with the AWS autoscaling API.
//
type Ec2AutoscaleNode struct {
	chainnode

	// Cluster is the ID of ec2 autoscale group to use.
	// The ID of the cluster is specified in the kapacitor configuration.
	Cluster string

	// GroupName is the name of the autoscaling group to autoscale.
	GroupName string
	// GroupName is the name of a tag which contains the name of the autoscaling group to autoscale.
	GroupNameTag string
	// OutputGroupName is the name of a tag into which the group name will be written for output autoscale events.
	// Defaults to the value of GroupNameTag if its not empty.
	OutputGroupNameTag string

	// CurrentField is the name of a field into which the current replica count will be set as an int.
	// If empty no field will be set.
	// Useful for computing deltas on the current state.
	//
	// Example:
	//    |ec2Autoscale()
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

func newEc2AutoscaleNode(e EdgeType) *Ec2AutoscaleNode {
	k := &Ec2AutoscaleNode{
		chainnode: newBasicChainNode("ec2_autoscale", e, StreamEdge),
		Min:       1,
	}
	return k
}

func (n *Ec2AutoscaleNode) validate() error {
	if (n.GroupName == "" && n.GroupNameTag == "") ||
		(n.GroupName != "" && n.GroupNameTag != "") {
		return fmt.Errorf("must specify exactly one of GroupName or GroupNameTag")
	}
	if n.Min < 1 {
		return fmt.Errorf("min must be >= 1, got %d", n.Min)
	}
	if n.Replicas == nil {
		return errors.New("must provide a replicas lambda expression")
	}
	return nil
}
