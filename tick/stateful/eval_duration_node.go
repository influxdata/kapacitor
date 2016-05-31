package stateful

import (
	"regexp"
	"time"

	"github.com/influxdata/kapacitor/tick/ast"
)

type EvalDurationNode struct {
	Duration time.Duration
}

func (n *EvalDurationNode) Type(scope ReadOnlyScope, executionState ExecutionState) (ast.ValueType, error) {
	return ast.TDuration, nil
}

func (n *EvalDurationNode) EvalFloat(scope *Scope, executionState ExecutionState) (float64, error) {
	return float64(0), ErrTypeGuardFailed{RequestedType: ast.TFloat, ActualType: ast.TDuration}
}

func (n *EvalDurationNode) EvalInt(scope *Scope, executionState ExecutionState) (int64, error) {
	return 0, ErrTypeGuardFailed{RequestedType: ast.TInt, ActualType: ast.TDuration}
}

func (n *EvalDurationNode) EvalString(scope *Scope, executionState ExecutionState) (string, error) {
	return "", ErrTypeGuardFailed{RequestedType: ast.TString, ActualType: ast.TDuration}
}

func (n *EvalDurationNode) EvalBool(scope *Scope, executionState ExecutionState) (bool, error) {
	return false, ErrTypeGuardFailed{RequestedType: ast.TBool, ActualType: ast.TDuration}
}
func (n *EvalDurationNode) EvalRegex(scope *Scope, executionState ExecutionState) (*regexp.Regexp, error) {
	return nil, ErrTypeGuardFailed{RequestedType: ast.TRegex, ActualType: ast.TDuration}
}
func (n *EvalDurationNode) EvalTime(scope *Scope, executionState ExecutionState) (time.Time, error) {
	return time.Time{}, ErrTypeGuardFailed{RequestedType: ast.TTime, ActualType: ast.TDuration}
}

func (n *EvalDurationNode) EvalDuration(scope *Scope, executionState ExecutionState) (time.Duration, error) {
	return n.Duration, nil
}
func (n *EvalDurationNode) IsDynamic() bool {
	return false
}
