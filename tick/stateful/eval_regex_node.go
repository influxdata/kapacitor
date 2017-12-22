package stateful

import (
	"regexp"
	"time"

	"github.com/influxdata/kapacitor/tick/ast"
)

type EvalRegexNode struct {
	Node *ast.RegexNode
}

func (n *EvalRegexNode) String() string {
	return n.Node.Literal
}

func (n *EvalRegexNode) Type(scope ReadOnlyScope) (ast.ValueType, error) {
	return ast.TRegex, nil
}

func (n *EvalRegexNode) EvalRegex(scope *Scope, executionState ExecutionState) (*regexp.Regexp, error) {
	return n.Node.Regex, nil
}

func (n *EvalRegexNode) EvalString(scope *Scope, executionState ExecutionState) (string, error) {
	return "", ErrTypeGuardFailed{RequestedType: ast.TString, ActualType: ast.TRegex}
}

func (n *EvalRegexNode) EvalFloat(scope *Scope, executionState ExecutionState) (float64, error) {
	return float64(0), ErrTypeGuardFailed{RequestedType: ast.TFloat, ActualType: ast.TRegex}
}

func (n *EvalRegexNode) EvalInt(scope *Scope, executionState ExecutionState) (int64, error) {
	return int64(0), ErrTypeGuardFailed{RequestedType: ast.TInt, ActualType: ast.TRegex}
}

func (n *EvalRegexNode) EvalBool(scope *Scope, executionState ExecutionState) (bool, error) {
	return false, ErrTypeGuardFailed{RequestedType: ast.TBool, ActualType: ast.TRegex}
}

func (n *EvalRegexNode) EvalTime(scope *Scope, executionState ExecutionState) (time.Time, error) {
	return time.Time{}, ErrTypeGuardFailed{RequestedType: ast.TTime, ActualType: ast.TRegex}
}

func (n *EvalRegexNode) EvalDuration(scope *Scope, executionState ExecutionState) (time.Duration, error) {
	return 0, ErrTypeGuardFailed{RequestedType: ast.TDuration, ActualType: ast.TRegex}
}

func (n *EvalRegexNode) EvalMissing(scope *Scope, executionState ExecutionState) (*ast.Missing, error) {
	return nil, ErrTypeGuardFailed{RequestedType: ast.TMissing, ActualType: ast.TRegex}
}

func (n *EvalRegexNode) IsDynamic() bool {
	return false
}
