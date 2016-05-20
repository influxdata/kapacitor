package stateful

import (
	"regexp"
	"time"

	"github.com/influxdata/kapacitor/tick/ast"
)

type EvalFloatNode struct {
	Float64 float64
}

func (n *EvalFloatNode) Type(scope ReadOnlyScope, executionState ExecutionState) (ast.ValueType, error) {
	return ast.TFloat, nil
}

func (n *EvalFloatNode) EvalFloat(scope *Scope, executionState ExecutionState) (float64, error) {
	return n.Float64, nil
}

func (n *EvalFloatNode) EvalInt(scope *Scope, executionState ExecutionState) (int64, error) {
	return int64(0), ErrTypeGuardFailed{RequestedType: ast.TFloat, ActualType: ast.TFloat}
}

func (n *EvalFloatNode) EvalString(scope *Scope, executionState ExecutionState) (string, error) {
	return "", ErrTypeGuardFailed{RequestedType: ast.TString, ActualType: ast.TFloat}
}

func (n *EvalFloatNode) EvalBool(scope *Scope, executionState ExecutionState) (bool, error) {
	return false, ErrTypeGuardFailed{RequestedType: ast.TBool, ActualType: ast.TFloat}
}
func (n *EvalFloatNode) EvalRegex(scope *Scope, executionState ExecutionState) (*regexp.Regexp, error) {
	return nil, ErrTypeGuardFailed{RequestedType: ast.TRegex, ActualType: ast.TFloat}
}
func (n *EvalFloatNode) EvalTime(scope *Scope, executionState ExecutionState) (time.Time, error) {
	return time.Time{}, ErrTypeGuardFailed{RequestedType: ast.TTime, ActualType: ast.TFloat}
}
func (n *EvalFloatNode) EvalDuration(scope *Scope, executionState ExecutionState) (time.Duration, error) {
	return 0, ErrTypeGuardFailed{RequestedType: ast.TDuration, ActualType: ast.TFloat}
}
func (n *EvalFloatNode) IsDynamic() bool {
	return false
}
