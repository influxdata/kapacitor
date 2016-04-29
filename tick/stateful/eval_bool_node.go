package stateful

import (
	"regexp"

	"github.com/influxdata/kapacitor/tick"
)

type EvalBoolNode struct {
	Node *tick.BoolNode
}

func (n *EvalBoolNode) Type(scope ReadOnlyScope, executionState ExecutionState) (ValueType, error) {
	return TBool, nil
}

func (n *EvalBoolNode) EvalBool(scope *tick.Scope, executionState ExecutionState) (bool, error) {
	return n.Node.Bool, nil
}

func (n *EvalBoolNode) EvalFloat(scope *tick.Scope, executionState ExecutionState) (float64, error) {
	return float64(0), ErrTypeGuardFailed{RequestedType: TFloat64, ActualType: TBool}
}

func (n *EvalBoolNode) EvalInt(scope *tick.Scope, executionState ExecutionState) (int64, error) {
	return int64(0), ErrTypeGuardFailed{RequestedType: TInt64, ActualType: TBool}
}

func (n *EvalBoolNode) EvalString(scope *tick.Scope, executionState ExecutionState) (string, error) {
	return "", ErrTypeGuardFailed{RequestedType: TString, ActualType: TBool}
}

func (n *EvalBoolNode) EvalRegex(scope *tick.Scope, executionState ExecutionState) (*regexp.Regexp, error) {
	return nil, ErrTypeGuardFailed{RequestedType: TRegex, ActualType: TBool}
}
