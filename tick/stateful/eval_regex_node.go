package stateful

import (
	"regexp"
	"time"

	"github.com/influxdata/kapacitor/tick"
)

type EvalRegexNode struct {
	Node *tick.RegexNode
}

func (n *EvalRegexNode) Type(scope ReadOnlyScope, executionState ExecutionState) (ValueType, error) {
	return TRegex, nil
}

func (n *EvalRegexNode) EvalRegex(scope *tick.Scope, executionState ExecutionState) (*regexp.Regexp, error) {
	return n.Node.Regex, nil
}

func (n *EvalRegexNode) EvalString(scope *tick.Scope, executionState ExecutionState) (string, error) {
	return "", ErrTypeGuardFailed{RequestedType: TString, ActualType: TRegex}
}

func (n *EvalRegexNode) EvalFloat(scope *tick.Scope, executionState ExecutionState) (float64, error) {
	return float64(0), ErrTypeGuardFailed{RequestedType: TFloat64, ActualType: TRegex}
}

func (n *EvalRegexNode) EvalInt(scope *tick.Scope, executionState ExecutionState) (int64, error) {
	return int64(0), ErrTypeGuardFailed{RequestedType: TInt64, ActualType: TRegex}
}

func (n *EvalRegexNode) EvalBool(scope *tick.Scope, executionState ExecutionState) (bool, error) {
	return false, ErrTypeGuardFailed{RequestedType: TBool, ActualType: TRegex}
}
func (n *EvalRegexNode) EvalTime(scope *tick.Scope, executionState ExecutionState) (time.Time, error) {
	return time.Time{}, ErrTypeGuardFailed{RequestedType: TTime, ActualType: TRegex}
}
