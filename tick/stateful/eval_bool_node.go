package stateful

import (
	"regexp"
	"time"

	"github.com/influxdata/kapacitor/tick/ast"
)

type EvalBoolNode struct {
	Node *ast.BoolNode
}

func (n *EvalBoolNode) Type(scope ReadOnlyScope, executionState ExecutionState) (ast.ValueType, error) {
	return ast.TBool, nil
}

func (n *EvalBoolNode) EvalBool(scope *Scope, executionState ExecutionState) (bool, error) {
	return n.Node.Bool, nil
}

func (n *EvalBoolNode) EvalFloat(scope *Scope, executionState ExecutionState) (float64, error) {
	return float64(0), ErrTypeGuardFailed{RequestedType: ast.TFloat, ActualType: ast.TBool}
}

func (n *EvalBoolNode) EvalInt(scope *Scope, executionState ExecutionState) (int64, error) {
	return int64(0), ErrTypeGuardFailed{RequestedType: ast.TInt, ActualType: ast.TBool}
}

func (n *EvalBoolNode) EvalString(scope *Scope, executionState ExecutionState) (string, error) {
	return "", ErrTypeGuardFailed{RequestedType: ast.TString, ActualType: ast.TBool}
}

func (n *EvalBoolNode) EvalRegex(scope *Scope, executionState ExecutionState) (*regexp.Regexp, error) {
	return nil, ErrTypeGuardFailed{RequestedType: ast.TRegex, ActualType: ast.TBool}
}

func (n *EvalBoolNode) EvalTime(scope *Scope, executionState ExecutionState) (time.Time, error) {
	return time.Time{}, ErrTypeGuardFailed{RequestedType: ast.TTime, ActualType: ast.TBool}
}
func (n *EvalBoolNode) EvalDuration(scope *Scope, executionState ExecutionState) (time.Duration, error) {
	return 0, ErrTypeGuardFailed{RequestedType: ast.TDuration, ActualType: ast.TBool}
}
func (n *EvalBoolNode) IsDynamic() bool {
	return false
}
