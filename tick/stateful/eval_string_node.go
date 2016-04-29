package stateful

import (
	"regexp"

	"github.com/influxdata/kapacitor/tick"
)

type EvalStringNode struct {
	Node *tick.StringNode
}

func (n *EvalStringNode) Type(scope ReadOnlyScope, executionState ExecutionState) (ValueType, error) {
	return TString, nil
}

func (n *EvalStringNode) EvalString(scope *tick.Scope, executionState ExecutionState) (string, error) {
	return n.Node.Literal, nil
}

func (n *EvalStringNode) EvalFloat(scope *tick.Scope, executionState ExecutionState) (float64, error) {
	return float64(0), ErrTypeGuardFailed{RequestedType: TFloat64, ActualType: TString}
}

func (n *EvalStringNode) EvalInt(scope *tick.Scope, executionState ExecutionState) (int64, error) {
	return int64(0), ErrTypeGuardFailed{RequestedType: TInt64, ActualType: TString}
}

func (n *EvalStringNode) EvalBool(scope *tick.Scope, executionState ExecutionState) (bool, error) {
	return false, ErrTypeGuardFailed{RequestedType: TBool, ActualType: TString}
}

func (n *EvalStringNode) EvalRegex(scope *tick.Scope, executionState ExecutionState) (*regexp.Regexp, error) {
	return nil, ErrTypeGuardFailed{RequestedType: TRegex, ActualType: TString}
}
