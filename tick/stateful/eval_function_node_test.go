package stateful_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/influxdata/kapacitor/tick/ast"
	"github.com/influxdata/kapacitor/tick/stateful"
)

func TestEvalFunctionNode_InvalidNodeAsArgument(t *testing.T) {
	evaluator, err := stateful.NewEvalFunctionNode(&ast.FunctionNode{
		Args: []ast.Node{&ast.CommentNode{}},
	})

	expectedError := errors.New("Failed to handle 1 argument: Given node type is not valid evaluation node: *ast.CommentNode")

	if err == nil && evaluator != nil {
		t.Error("Expected an error, but go nil error and evaluator")
		return
	}

	if err.Error() != expectedError.Error() {
		t.Errorf("Got unexpected error:\ngot: %v\nexpected: %v\n", err, expectedError)
	}
}

func TestEvalFunctionNode_FailedToEvaluateArgumentNodes(t *testing.T) {
	// bool("value"), where in our case "value" won't exist
	evaluator, err := stateful.NewEvalFunctionNode(&ast.FunctionNode{
		Func: "bool",
		Args: []ast.Node{
			&ast.ReferenceNode{Reference: "value"},
		},
	})

	if err != nil {
		t.Fatalf("Failed to create node evaluator: %v", err)
	}

	result, err := evaluator.EvalBool(stateful.NewScope(), stateful.CreateExecutionState())

	expectedError := errors.New("Failed to handle 1 argument: name \"value\" is undefined. Names in scope:")
	if err == nil {
		t.Errorf("Expected an error, but got nil error and result (%t)", result)
		return
	}

	if strings.TrimSpace(err.Error()) != expectedError.Error() {
		t.Errorf("Got unexpected error:\ngot: %v\nexpected: %v\n", err, expectedError)
	}
}

func TestEvalFunctionNode_UndefinedFunction(t *testing.T) {
	evaluator, err := stateful.NewEvalFunctionNode(&ast.FunctionNode{
		Func: "yosi_the_king",
		Args: []ast.Node{},
	})

	if err != nil {
		t.Fatalf("Failed to create node evaluator: %v", err)
	}

	result, err := evaluator.EvalBool(stateful.NewScope(), stateful.CreateExecutionState())

	expectedError := errors.New("undefined function: \"yosi_the_king\"")
	if err == nil {
		t.Errorf("Expected an error, but got nil error and result (%t)", result)
		return
	}

	if strings.TrimSpace(err.Error()) != expectedError.Error() {
		t.Errorf("Got unexpected error:\ngot: %v\nexpected: %v\n", err, expectedError)
	}
}

func TestEvalFunctionNode_AryMismatch(t *testing.T) {
	evaluator, err := stateful.NewEvalFunctionNode(&ast.FunctionNode{
		Func: "sigma",
		Args: []ast.Node{&ast.BoolNode{Bool: true}},
	})

	if err != nil {
		t.Fatalf("Failed to create node evaluator: %v", err)
	}

	result, err := evaluator.EvalFloat(stateful.NewScope(), stateful.CreateExecutionState())

	expectedError := errors.New("error calling \"sigma\": value is not a float")
	if err == nil {
		t.Errorf("Expected an error, but got nil error and result (%v)", result)
		return
	}

	if strings.TrimSpace(err.Error()) != expectedError.Error() {
		t.Errorf("Got unexpected error:\ngot: %v\nexpected: %v\n", err, expectedError)
	}
}

func TestEvalFunctionNode_EvalBool_Sanity(t *testing.T) {
	evaluator, err := stateful.NewEvalFunctionNode(&ast.FunctionNode{
		Func: "bool",
		Args: []ast.Node{&ast.StringNode{Literal: "true"}},
	})

	if err != nil {
		t.Fatalf("Failed to create node evaluator: %v", err)
	}

	result, err := evaluator.EvalBool(stateful.NewScope(), stateful.CreateExecutionState())
	if err != nil {
		t.Errorf("Expected a result, but got error - %v", err)
		return
	}

	if !result {
		t.Errorf("unexpected result: got: %v, expected: true", result)
	}
}

func TestEvalFunctionNode_EvalInt64_Sanity(t *testing.T) {
	evaluator, err := stateful.NewEvalFunctionNode(&ast.FunctionNode{
		Func: "count",
		Args: []ast.Node{},
	})

	if err != nil {
		t.Fatalf("Failed to create node evaluator: %v", err)
	}

	result, err := evaluator.EvalInt(stateful.NewScope(), stateful.CreateExecutionState())
	if err != nil {
		t.Errorf("Expected a result, but got error - %v", err)
		return
	}

	if result != int64(1) {
		t.Errorf("unexpected result: got: %T(%v), expected: int64(1)", result, result)
	}
}

func TestEvalFunctionNode_EvalInt64_KeepConsistentState(t *testing.T) {
	evaluator, err := stateful.NewEvalFunctionNode(&ast.FunctionNode{
		Func: "count",
		Args: []ast.Node{},
	})

	if err != nil {
		t.Fatalf("Failed to create node evaluator: %v", err)
	}

	executionState := stateful.CreateExecutionState()

	// first evaluation
	result, err := evaluator.EvalInt(stateful.NewScope(), executionState)
	if err != nil {
		t.Errorf("first evaluation: Expected a result, but got error - %v", err)
		return
	}

	if result != int64(1) {
		t.Errorf("first evaluation: unexpected result: got: %T(%v), expected: int64(1)", result, result)
	}

	// second evaluation
	result, err = evaluator.EvalInt(stateful.NewScope(), executionState)
	if err != nil {
		t.Errorf("second evaluation: Expected a result, but got error - %v", err)
		return
	}

	if result != int64(2) {
		t.Errorf("second evaluation: unexpected result: got: %T(%v), expected: int64(2)", result, result)
	}
}

func TestEvalFunctionNode_EvalInt64_Reset(t *testing.T) {
	evaluator, err := stateful.NewEvalFunctionNode(&ast.FunctionNode{
		Func: "count",
		Args: []ast.Node{},
	})

	if err != nil {
		t.Fatalf("Failed to create node evaluator: %v", err)
	}

	executionState := stateful.CreateExecutionState()

	// first evaluation
	result, err := evaluator.EvalInt(stateful.NewScope(), executionState)
	if err != nil {
		t.Errorf("first evaluation: Expected a result, but got error - %v", err)
		return
	}

	if result != int64(1) {
		t.Errorf("first evaluation: unexpected result: got: %T(%v), expected: int64(1)", result, result)
	}

	// reset (we don't call ResetAll on ExecutionState in order to isolate the scope of this test)
	for _, fnc := range executionState.Funcs {
		fnc.Reset()
	}

	// second evaluation
	result, err = evaluator.EvalInt(stateful.NewScope(), executionState)
	if err != nil {
		t.Errorf("second evaluation (after reset): Expected a result, but got error - %v", err)
		return
	}

	if result != int64(1) {
		t.Errorf("second evaluation (after reset): unexpected result: got: %T(%v), expected: int64(1)", result, result)
	}
}

func TestEvalFunctionNode_EvalFloat64_Sanity(t *testing.T) {
	evaluator, err := stateful.NewEvalFunctionNode(&ast.FunctionNode{
		Func: "abs",
		Args: []ast.Node{
			&ast.NumberNode{
				IsFloat: true,
				Float64: float64(-1),
			},
		},
	})

	if err != nil {
		t.Fatalf("Failed to create node evaluator: %v", err)
	}

	result, err := evaluator.EvalFloat(stateful.NewScope(), stateful.CreateExecutionState())
	if err != nil {
		t.Errorf("Expected a result, but got error - %v", err)
		return
	}

	if result != float64(1) {
		t.Errorf("unexpected result: got: %T(%v), expected: float64(1)", result, result)
	}
}

func TestEvalFunctionNode_ComplexNodes(t *testing.T) {
	// pow("x" * 2, -"y") => pow(4, -1)
	evaluator, err := stateful.NewEvalFunctionNode(&ast.FunctionNode{
		Func: "pow",
		Args: []ast.Node{
			// "x" * 2
			&ast.BinaryNode{
				Operator: ast.TokenMult,
				Left:     &ast.ReferenceNode{Reference: "x"},
				Right:    &ast.NumberNode{IsFloat: true, Float64: float64(2)},
			},

			// -"y"
			&ast.UnaryNode{
				Operator: ast.TokenMinus,
				Node:     &ast.ReferenceNode{Reference: "y"},
			},
		},
	})

	if err != nil {
		t.Fatalf("Failed to create node evaluator: %v", err)
	}

	scope := stateful.NewScope()
	scope.Set("x", float64(2))
	scope.Set("y", float64(1))

	result, err := evaluator.EvalFloat(scope, stateful.CreateExecutionState())
	if err != nil {
		t.Errorf("Expected a result, but got error - %v", err)
		return
	}

	if result != float64(0.25) {
		t.Errorf("unexpected result: got: %T(%v), expected: float64(0.25)", result, result)
	}
}

func TestStatefulExpression_Integration_EvalBool_SanityCallingFunction(t *testing.T) {
	scope := stateful.NewScope()

	se := mustCompileExpression(&ast.BinaryNode{
		Operator: ast.TokenEqual,
		Left: &ast.FunctionNode{
			Func: "count",
		},
		Right: &ast.NumberNode{
			IsInt: true,
			Int64: 1,
		},
	})

	result, err := se.EvalBool(scope)
	if err != nil {
		t.Error(err)
	}

	if !result {
		t.Errorf("first evaluation: unexpected result: got: %v, expected: true", result)
	}

	// Second time, to make sure that count() increases the value
	result, err = se.EvalBool(scope)
	if err != nil {
		t.Error(err)
	}

	if result {
		t.Errorf("second evaluation: unexpected result: got: %v, expected: false", result)
	}

	// reset the expression
	se.Reset()

	result, err = se.EvalBool(scope)
	if err != nil {
		t.Error(err)
	}

	if !result {
		t.Errorf("last evaluation after reset: unexpected result: got: %v, expected: true", result)
	}

}
