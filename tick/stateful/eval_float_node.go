package stateful

import (
	"regexp"

	"github.com/influxdata/kapacitor/tick"
)

type EvalFloatNode struct {
	Float64 float64
}

func (n *EvalFloatNode) Type(scope ReadOnlyScope, executionState ExecutionState) (ValueType, error) {
	return TFloat64, nil
}

func (n *EvalFloatNode) EvalFloat(scope *tick.Scope, executionState ExecutionState) (float64, error) {
	return n.Float64, nil
}

func (n *EvalFloatNode) EvalInt(scope *tick.Scope, executionState ExecutionState) (int64, error) {
	return int64(0), ErrTypeGuardFailed{RequestedType: TFloat64, ActualType: TFloat64}
}

func (n *EvalFloatNode) EvalString(scope *tick.Scope, executionState ExecutionState) (string, error) {
	return "", ErrTypeGuardFailed{RequestedType: TString, ActualType: TFloat64}
}

func (n *EvalFloatNode) EvalBool(scope *tick.Scope, executionState ExecutionState) (bool, error) {
	return false, ErrTypeGuardFailed{RequestedType: TBool, ActualType: TFloat64}
}
func (n *EvalFloatNode) EvalRegex(scope *tick.Scope, executionState ExecutionState) (*regexp.Regexp, error) {
	return nil, ErrTypeGuardFailed{RequestedType: TRegex, ActualType: TFloat64}
}
