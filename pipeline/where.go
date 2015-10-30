package pipeline

import (
	"github.com/influxdb/kapacitor/tick"
)

// The WhereNode filters the data stream by a given expression.
//
// Example:
// var sums = stream
//     .groupBy('service', 'host')
//     .mapReduce(influxdb.sum('value'))
// //Watch particular host for issues.
// sums
//    .where(lambda: "host" == 'h001.example.com')
//    .alert()
//        .crit(lambda: TRUE)
//        .email('user@example.com')
//
type WhereNode struct {
	chainnode
	// The expression predicate.
	// tick:ignore
	Expression tick.Node
}

func newWhereNode(wants EdgeType, predicate tick.Node) *WhereNode {
	return &WhereNode{
		chainnode:  newBasicChainNode("where", wants, wants),
		Expression: predicate,
	}
}
