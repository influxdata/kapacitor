package stateful

import (
	"regexp"
	"time"

	"github.com/influxdata/kapacitor/tick/ast"
)

type EvalStringNode struct {
	Node *ast.StringNode
}

func (n *EvalStringNode) Type(scope ReadOnlyScope, executionState ExecutionState) (ast.ValueType, error) {
	return ast.TString, nil
}

func (n *EvalStringNode) EvalString(scope *Scope, executionState ExecutionState) (string, error) {
	return n.Node.Literal, nil
}

func (n *EvalStringNode) EvalFloat(scope *Scope, executionState ExecutionState) (float64, error) {
	return float64(0), ErrTypeGuardFailed{RequestedType: ast.TFloat, ActualType: ast.TString}
}

func (n *EvalStringNode) EvalInt(scope *Scope, executionState ExecutionState) (int64, error) {
	return int64(0), ErrTypeGuardFailed{RequestedType: ast.TInt, ActualType: ast.TString}
}

func (n *EvalStringNode) EvalBool(scope *Scope, executionState ExecutionState) (bool, error) {
	return false, ErrTypeGuardFailed{RequestedType: ast.TBool, ActualType: ast.TString}
}

func (n *EvalStringNode) EvalRegex(scope *Scope, executionState ExecutionState) (*regexp.Regexp, error) {
	return nil, ErrTypeGuardFailed{RequestedType: ast.TRegex, ActualType: ast.TString}
}
func (n *EvalStringNode) EvalTime(scope *Scope, executionState ExecutionState) (time.Time, error) {
	return time.Time{}, ErrTypeGuardFailed{RequestedType: ast.TTime, ActualType: ast.TString}
}
func (n *EvalStringNode) EvalDuration(scope *Scope, executionState ExecutionState) (time.Duration, error) {
	return 0, ErrTypeGuardFailed{RequestedType: ast.TDuration, ActualType: ast.TString}
}
func (n *EvalStringNode) IsDynamic() bool {
	return false
}
