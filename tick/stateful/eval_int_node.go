package stateful

import (
	"regexp"
	"time"

	"github.com/influxdata/kapacitor/tick"
)

type EvalIntNode struct {
	Int64 int64
}

func (n *EvalIntNode) Type(scope ReadOnlyScope, executionState ExecutionState) (ValueType, error) {
	return TInt64, nil
}

func (n *EvalIntNode) EvalFloat(scope *tick.Scope, executionState ExecutionState) (float64, error) {
	return float64(0), ErrTypeGuardFailed{RequestedType: TFloat64, ActualType: TInt64}
}

func (n *EvalIntNode) EvalInt(scope *tick.Scope, executionState ExecutionState) (int64, error) {
	return n.Int64, nil
}

func (n *EvalIntNode) EvalString(scope *tick.Scope, executionState ExecutionState) (string, error) {
	return "", ErrTypeGuardFailed{RequestedType: TString, ActualType: TInt64}
}

func (n *EvalIntNode) EvalBool(scope *tick.Scope, executionState ExecutionState) (bool, error) {
	return false, ErrTypeGuardFailed{RequestedType: TBool, ActualType: TInt64}
}
func (n *EvalIntNode) EvalRegex(scope *tick.Scope, executionState ExecutionState) (*regexp.Regexp, error) {
	return nil, ErrTypeGuardFailed{RequestedType: TRegex, ActualType: TInt64}
}
func (n *EvalIntNode) EvalTime(scope *tick.Scope, executionState ExecutionState) (time.Time, error) {
	return time.Time{}, ErrTypeGuardFailed{RequestedType: TTime, ActualType: TInt64}
}
