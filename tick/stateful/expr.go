package stateful

import (
	"fmt"
	"time"

	"github.com/influxdata/kapacitor/tick/ast"
)

// Expression is interface that describe expression with state and
// it's evaluation.
type Expression interface {
	Reset()

	EvalFloat(scope *Scope) (float64, error)
	EvalInt(scope *Scope) (int64, error)
	EvalString(scope *Scope) (string, error)
	EvalBool(scope *Scope) (bool, error)
	EvalDuration(scope *Scope) (time.Duration, error)

	Eval(scope *Scope) (interface{}, error)

	// Return a copy of the expression but with a Reset state.
	CopyReset() Expression
}

type expression struct {
	nodeEvaluator  NodeEvaluator
	executionState ExecutionState
}

// NewExpression accept a node and try to "compile"/ "specialise" it
// in order to achieve better runtime performance.
//
// For example:
// 	Given a BinaryNode{ReferNode("value"), NumberNode{Float64:10}} during runtime
// 	we can find the type of "value" and find the most matching comparison function - (float64,float64) or (int64,float64)
func NewExpression(node ast.Node) (Expression, error) {
	nodeEvaluator, err := createNodeEvaluator(node)
	if err != nil {
		return nil, err
	}

	return &expression{
		nodeEvaluator:  nodeEvaluator,
		executionState: CreateExecutionState(),
	}, nil
}

func (se *expression) CopyReset() Expression {
	return &expression{
		nodeEvaluator:  se.nodeEvaluator,
		executionState: CreateExecutionState(),
	}
}

func (se *expression) Reset() {
	se.executionState.ResetAll()
}

func (se *expression) EvalBool(scope *Scope) (bool, error) {
	return se.nodeEvaluator.EvalBool(scope, se.executionState)
}

func (se *expression) EvalInt(scope *Scope) (int64, error) {
	return se.nodeEvaluator.EvalInt(scope, se.executionState)
}

func (se *expression) EvalFloat(scope *Scope) (float64, error) {
	return se.nodeEvaluator.EvalFloat(scope, se.executionState)
}

func (se *expression) EvalString(scope *Scope) (string, error) {
	return se.nodeEvaluator.EvalString(scope, se.executionState)
}

func (se *expression) EvalDuration(scope *Scope) (time.Duration, error) {
	return se.nodeEvaluator.EvalDuration(scope, se.executionState)
}

func (se *expression) Eval(scope *Scope) (interface{}, error) {
	typ, err := se.nodeEvaluator.Type(scope, CreateExecutionState())
	if err != nil {
		return nil, err
	}

	switch typ {
	case ast.TInt:
		result, err := se.EvalInt(scope)
		if err != nil {
			return nil, err
		}
		return result, err
	case ast.TFloat:
		result, err := se.EvalFloat(scope)
		if err != nil {
			return nil, err
		}
		return result, err
	case ast.TString:
		result, err := se.EvalString(scope)
		if err != nil {
			return nil, err
		}
		return result, err
	case ast.TBool:
		result, err := se.EvalBool(scope)
		if err != nil {
			return nil, err
		}
		return result, err
	case ast.TDuration:
		result, err := se.EvalDuration(scope)
		if err != nil {
			return nil, err
		}
		return result, err
	default:
		return nil, fmt.Errorf("expression returned unexpected type %s", typ)
	}
}

func FindReferenceVariables(nodes ...ast.Node) []string {
	variablesSet := make(map[string]bool, 0)

	for _, node := range nodes {
		buildReferenceVariablesSet(node, variablesSet)
	}

	variables := make([]string, 0, len(variablesSet))

	for variable := range variablesSet {
		variables = append(variables, variable)
	}

	return variables
}

// util method for findReferenceVariables, we are passing the itemsSet and not returning it
// so we will won't to merge the maps
func buildReferenceVariablesSet(n ast.Node, itemsSet map[string]bool) {
	switch node := n.(type) {
	case *ast.ReferenceNode:
		itemsSet[node.Reference] = true
	case *ast.UnaryNode:
		buildReferenceVariablesSet(node.Node, itemsSet)
	case *ast.BinaryNode:
		buildReferenceVariablesSet(node.Left, itemsSet)
		buildReferenceVariablesSet(node.Right, itemsSet)
	case *ast.FunctionNode:
		for _, arg := range node.Args {
			buildReferenceVariablesSet(arg, itemsSet)
		}
	}
}
