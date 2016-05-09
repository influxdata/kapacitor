package stateful

import (
	"fmt"

	"github.com/influxdata/kapacitor/tick"
)

// Expression is interface that describe expression with state and
// it's evaluation.
type Expression interface {
	Reset()

	EvalFloat(scope *tick.Scope) (float64, error)
	EvalInt(scope *tick.Scope) (int64, error)
	EvalString(scope *tick.Scope) (string, error)
	EvalBool(scope *tick.Scope) (bool, error)

	Eval(scope *tick.Scope) (interface{}, error)
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
func NewExpression(node tick.Node) (Expression, error) {
	nodeEvaluator, err := createNodeEvaluator(node)
	if err != nil {
		return nil, err
	}

	return &expression{
		nodeEvaluator:  nodeEvaluator,
		executionState: CreateExecutionState(),
	}, nil
}

func (se *expression) Reset() {
	se.executionState.ResetAll()
}

func (se *expression) EvalBool(scope *tick.Scope) (bool, error) {
	return se.nodeEvaluator.EvalBool(scope, se.executionState)
}

func (se *expression) EvalInt(scope *tick.Scope) (int64, error) {
	return se.nodeEvaluator.EvalInt(scope, se.executionState)
}

func (se *expression) EvalFloat(scope *tick.Scope) (float64, error) {
	return se.nodeEvaluator.EvalFloat(scope, se.executionState)
}

func (se *expression) EvalString(scope *tick.Scope) (string, error) {
	return se.nodeEvaluator.EvalString(scope, se.executionState)
}

func (se *expression) Eval(scope *tick.Scope) (interface{}, error) {
	typ, err := se.nodeEvaluator.Type(scope, CreateExecutionState())
	if err != nil {
		return nil, err
	}

	switch typ {
	case TInt64:
		result, err := se.EvalInt(scope)
		if err != nil {
			return nil, err
		}
		return result, err
	case TFloat64:
		result, err := se.EvalFloat(scope)
		if err != nil {
			return nil, err
		}
		return result, err
	case TString:
		result, err := se.EvalString(scope)
		if err != nil {
			return nil, err
		}
		return result, err
	case TBool:
		result, err := se.EvalBool(scope)
		if err != nil {
			return nil, err
		}
		return result, err
	default:
		return nil, fmt.Errorf("expression returned unexpected type %s", typ)
	}
}

func FindReferenceVariables(nodes ...tick.Node) []string {

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
func buildReferenceVariablesSet(n tick.Node, itemsSet map[string]bool) {
	switch node := n.(type) {
	case *tick.ReferenceNode:
		itemsSet[node.Reference] = true
	case *tick.UnaryNode:
		buildReferenceVariablesSet(node.Node, itemsSet)
	case *tick.BinaryNode:
		buildReferenceVariablesSet(node.Left, itemsSet)
		buildReferenceVariablesSet(node.Right, itemsSet)
	case *tick.FunctionNode:
		for _, arg := range node.Args {
			buildReferenceVariablesSet(arg, itemsSet)
		}
	}
}
