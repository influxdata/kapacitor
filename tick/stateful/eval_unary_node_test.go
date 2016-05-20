package stateful_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/kapacitor/tick/ast"
	"github.com/influxdata/kapacitor/tick/stateful"
)

func TestEvalUnaryNode_InvalidOperator(t *testing.T) {
	evaluator, err := stateful.NewEvalUnaryNode(&ast.UnaryNode{
		Operator: ast.TokenAnd,
		Node:     &ast.BoolNode{Bool: true},
	})

	expectedError := errors.New("Invalid unary operator: \"AND\"")

	if err == nil && evaluator != nil {
		t.Error("Expected an error, but got nil error and evaluator")
		return
	}

	if err.Error() != expectedError.Error() {
		t.Errorf("Got unexpected error:\ngot: %v\nexpected: %v\n", err, expectedError)
	}
}

func TestEvalUnaryNode_InvalidNode(t *testing.T) {
	evaluator, err := stateful.NewEvalUnaryNode(&ast.UnaryNode{
		Operator: ast.TokenMinus,
		Node:     &ast.CommentNode{},
	})

	expectedError := errors.New("Failed to handle node: Given node type is not valid evaluation node: *ast.CommentNode")

	if err == nil && evaluator != nil {
		t.Error("Expected an error, but got nil error and evaluator")
		return
	}

	if err.Error() != expectedError.Error() {
		t.Errorf("Got unexpected error:\ngot: %v\nexpected: %v\n", err, expectedError)
	}
}

func TestEvalUnaryNode_EvalBool_Sanity(t *testing.T) {
	evaluator, err := stateful.NewEvalUnaryNode(&ast.UnaryNode{
		Operator: ast.TokenNot,
		Node: &ast.BoolNode{
			Bool: false,
		},
	})

	if err != nil {
		t.Fatalf("Failed to compile unary node: %v", err)
	}

	result, err := evaluator.EvalBool(stateful.NewScope(), stateful.CreateExecutionState())
	if err != nil {
		t.Errorf("Got unexpected error: %v", err)
	}

	if !result {
		t.Errorf("unexpected result: got: %t, expected: true", result)
	}
}

func TestEvalUnaryNode_EvalFloat64_Sanity(t *testing.T) {
	evaluator, err := stateful.NewEvalUnaryNode(&ast.UnaryNode{
		Operator: ast.TokenMinus,
		Node: &ast.NumberNode{
			IsFloat: true,
			Float64: float64(12),
		},
	})

	if err != nil {
		t.Fatalf("Failed to compile unary node: %v", err)
	}

	result, err := evaluator.EvalFloat(stateful.NewScope(), stateful.CreateExecutionState())
	if err != nil {
		t.Errorf("Got unexpected error: %v", err)
	}

	if result != float64(-12) {
		t.Errorf("unexpected result: got: %T(%v), expected: float64(-12)", result, result)
	}
}

func TestEvalUnaryNode_EvalInt64_Sanity(t *testing.T) {
	evaluator, err := stateful.NewEvalUnaryNode(&ast.UnaryNode{
		Operator: ast.TokenMinus,
		Node: &ast.NumberNode{
			IsInt: true,
			Int64: int64(12),
		},
	})

	if err != nil {
		t.Fatalf("Failed to compile unary node: %v", err)
	}

	result, err := evaluator.EvalInt(stateful.NewScope(), stateful.CreateExecutionState())
	if err != nil {
		t.Errorf("Got unexpected error: %v", err)
	}

	if result != int64(-12) {
		t.Errorf("unexpected result: got: %T(%v), expected: int64(-12)", result, result)
	}
}

func TestEvalUnaryNode_EvalDuration_Sanity(t *testing.T) {
	evaluator, err := stateful.NewEvalUnaryNode(&ast.UnaryNode{
		Operator: ast.TokenMinus,
		Node: &ast.DurationNode{
			Dur: 12 * time.Hour,
		},
	})

	if err != nil {
		t.Fatalf("Failed to compile unary node: %v", err)
	}

	result, err := evaluator.EvalDuration(stateful.NewScope(), stateful.CreateExecutionState())
	if err != nil {
		t.Errorf("Got unexpected error: %v", err)
	}

	if got, exp := result, -12*time.Hour; got != exp {
		t.Errorf("unexpected result: got: %T(%v), expected: %s", got, got, exp)
	}
}

func TestEvalUnaryNode_EvalString_UnexpectedReturnType(t *testing.T) {
	evaluator, err := stateful.NewEvalUnaryNode(&ast.UnaryNode{
		Operator: ast.TokenNot,
		Node: &ast.BoolNode{
			Bool: false,
		},
	})

	if err != nil {
		t.Fatalf("Failed to compile unary node: %v", err)
	}

	result, err := evaluator.EvalString(stateful.NewScope(), stateful.CreateExecutionState())

	expectedError := errors.New("TypeGuard: expression returned unexpected type boolean, expected string")

	if err == nil && result != "" {
		t.Errorf("Expected an error, but got nil error and result: %v", result)
		return
	}

	if err.Error() != expectedError.Error() {
		t.Errorf("Got unexpected error:\ngot: %v\nexpected: %v\n", err, expectedError)
	}
}

func TestEvalUnaryNode_EvalInt64_FailedToEvaluateNode(t *testing.T) {
	evaluator, err := stateful.NewEvalUnaryNode(&ast.UnaryNode{
		Operator: ast.TokenMinus,
		Node: &ast.BoolNode{
			Bool: false,
		},
	})

	if err != nil {
		t.Fatalf("Failed to compile unary node: %v", err)
	}

	result, err := evaluator.EvalInt(stateful.NewScope(), stateful.CreateExecutionState())

	expectedError := errors.New("TypeGuard: expression returned unexpected type boolean, expected int")

	if err == nil && result != int64(0) {
		t.Errorf("Expected an error, but got nil error and result: %v", result)
		return
	}

	if err.Error() != expectedError.Error() {
		t.Errorf("Got unexpected error:\ngot: %v\nexpected: %v\n", err, expectedError)
	}
}

func TestEvalUnaryNode_EvalFloat64_FailedToEvaluateNode(t *testing.T) {
	evaluator, err := stateful.NewEvalUnaryNode(&ast.UnaryNode{
		Operator: ast.TokenMinus,
		Node: &ast.ReferenceNode{
			Reference: "value",
		},
	})

	if err != nil {
		t.Fatalf("Failed to compile unary node: %v", err)
	}

	result, err := evaluator.EvalFloat(stateful.NewScope(), stateful.CreateExecutionState())

	expectedError := errors.New("name \"value\" is undefined. Names in scope:")

	if err == nil && result != float64(0) {
		t.Errorf("Expected an error, but got nil error and result: %v", result)
		return
	}

	if strings.TrimSpace(err.Error()) != expectedError.Error() {
		t.Errorf("Got unexpected error:\ngot: %v\nexpected: %v\n", err, expectedError)
	}
}
