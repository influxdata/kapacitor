package stateful

import (
	"regexp"
	"time"

	"github.com/influxdata/kapacitor/tick/ast"
)

type EvalIntNode struct {
	Int64 int64
}

func (n *EvalIntNode) Type(scope ReadOnlyScope, executionState ExecutionState) (ast.ValueType, error) {
	return ast.TInt, nil
}

func (n *EvalIntNode) EvalFloat(scope *Scope, executionState ExecutionState) (float64, error) {
	return float64(0), ErrTypeGuardFailed{RequestedType: ast.TFloat, ActualType: ast.TInt}
}

func (n *EvalIntNode) EvalInt(scope *Scope, executionState ExecutionState) (int64, error) {
	return n.Int64, nil
}

func (n *EvalIntNode) EvalString(scope *Scope, executionState ExecutionState) (string, error) {
	return "", ErrTypeGuardFailed{RequestedType: ast.TString, ActualType: ast.TInt}
}

func (n *EvalIntNode) EvalBool(scope *Scope, executionState ExecutionState) (bool, error) {
	return false, ErrTypeGuardFailed{RequestedType: ast.TBool, ActualType: ast.TInt}
}
func (n *EvalIntNode) EvalRegex(scope *Scope, executionState ExecutionState) (*regexp.Regexp, error) {
	return nil, ErrTypeGuardFailed{RequestedType: ast.TRegex, ActualType: ast.TInt}
}
func (n *EvalIntNode) EvalTime(scope *Scope, executionState ExecutionState) (time.Time, error) {
	return time.Time{}, ErrTypeGuardFailed{RequestedType: ast.TTime, ActualType: ast.TInt}
}
func (n *EvalIntNode) EvalDuration(scope *Scope, executionState ExecutionState) (time.Duration, error) {
	return 0, ErrTypeGuardFailed{RequestedType: ast.TDuration, ActualType: ast.TInt}
}
func (n *EvalIntNode) IsDynamic() bool {
	return false
}
