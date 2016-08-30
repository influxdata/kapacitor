package pipeline

import (
	"time"
)

// Compute the derivative of a stream or batch.
// The derivative is computed on a single field
// and behaves similarly to the InfluxQL derivative
// function. Kapacitor has its own implementation
// of the derivative function, and, as a result, is
// not part of the normal InfluxQL functions.
//
// Example:
//     stream
//         |from()
//             .measurement('net_rx_packets')
//         |derivative('value')
//            .unit(1s) // default
//            .nonNegative()
//         ...
//
// Computes the derivative via:
//    (current - previous ) / ( time_difference / unit)
//
// For batch edges the derivative is computed for each
// point in the batch and because of boundary conditions
// the number of points is reduced by one.
type DerivativeNode struct {
	chainnode

	// The field to use when calculating the derivative
	// tick:ignore
	Field string

	// The new name of the derivative field.
	// Default is the name of the field used
	// when calculating the derivative.
	As string

	// The time unit of the resulting derivative value.
	// Default: 1s
	Unit time.Duration

	// Where negative values are acceptable.
	// tick:ignore
	NonNegativeFlag bool `tick:"NonNegative"`
}

func newDerivativeNode(wants EdgeType, field string) *DerivativeNode {
	return &DerivativeNode{
		chainnode: newBasicChainNode("derivative", wants, wants),
		Unit:      time.Second,
		Field:     field,
		As:        field,
	}
}

// If called the derivative will skip negative results.
// tick:property
func (d *DerivativeNode) NonNegative() *DerivativeNode {
	d.NonNegativeFlag = true
	return d
}
