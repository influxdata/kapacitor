package pipeline

import (
	"time"
)

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
	NonNegativeFlag bool
}

func newDerivativeNode(wants EdgeType, field string) *DerivativeNode {
	return &DerivativeNode{
		chainnode: newBasicChainNode("derivative", wants, wants),
		Unit:      time.Second,
		Field:     field,
		As:        field,
	}
}

// If called the derivative will never return a negative
// value.
// tick:property
func (d *DerivativeNode) NonNegative() *DerivativeNode {
	d.NonNegativeFlag = true
	return d
}
