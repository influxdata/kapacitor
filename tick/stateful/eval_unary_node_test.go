package stateful_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/influxdata/kapacitor/tick"
	"github.com/influxdata/kapacitor/tick/stateful"
)

func TestEvalUnaryNode_InvalidOperator(t *testing.T) {
	evaluator, err := stateful.NewEvalUnaryNode(&tick.UnaryNode{
		Operator: tick.TokenAnd,
		Node:     &tick.BoolNode{Bool: true},
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
	evaluator, err := stateful.NewEvalUnaryNode(&tick.UnaryNode{
		Operator: tick.TokenMinus,
		Node:     &tick.CommentNode{},
	})

	expectedError := errors.New("Failed to handle node: Given node type is not valid evaluation node: *tick.CommentNode")

	if err == nil && evaluator != nil {
		t.Error("Expected an error, but got nil error and evaluator")
		return
	}

	if err.Error() != expectedError.Error() {
		t.Errorf("Got unexpected error:\ngot: %v\nexpected: %v\n", err, expectedError)
	}
}

func TestEvalUnaryNode_EvalBool_Sanity(t *testing.T) {
	evaluator, err := stateful.NewEvalUnaryNode(&tick.UnaryNode{
		Operator: tick.TokenNot,
		Node: &tick.BoolNode{
			Bool: false,
		},
	})

	if err != nil {
		t.Fatalf("Failed to compile unary node: %v", err)
	}

	result, err := evaluator.EvalBool(tick.NewScope(), stateful.CreateExecutionState())
	if err != nil {
		t.Errorf("Got unexpected error: %v", err)
	}

	if !result {
		t.Errorf("unexpected result: got: %t, expected: true", result)
	}
}

func TestEvalUnaryNode_EvalFloat64_Sanity(t *testing.T) {
	evaluator, err := stateful.NewEvalUnaryNode(&tick.UnaryNode{
		Operator: tick.TokenMinus,
		Node: &tick.NumberNode{
			IsFloat: true,
			Float64: float64(12),
		},
	})

	if err != nil {
		t.Fatalf("Failed to compile unary node: %v", err)
	}

	result, err := evaluator.EvalFloat(tick.NewScope(), stateful.CreateExecutionState())
	if err != nil {
		t.Errorf("Got unexpected error: %v", err)
	}

	if result != float64(-12) {
		t.Errorf("unexpected result: got: %T(%v), expected: float64(-12)", result, result)
	}
}

func TestEvalUnaryNode_EvalInt64_Sanity(t *testing.T) {
	evaluator, err := stateful.NewEvalUnaryNode(&tick.UnaryNode{
		Operator: tick.TokenMinus,
		Node: &tick.NumberNode{
			IsInt: true,
			Int64: int64(12),
		},
	})

	if err != nil {
		t.Fatalf("Failed to compile unary node: %v", err)
	}

	result, err := evaluator.EvalInt(tick.NewScope(), stateful.CreateExecutionState())
	if err != nil {
		t.Errorf("Got unexpected error: %v", err)
	}

	if result != int64(-12) {
		t.Errorf("unexpected result: got: %T(%v), expected: int64(-12)", result, result)
	}
}

func TestEvalUnaryNode_EvalString_UnexpectedReturnType(t *testing.T) {
	evaluator, err := stateful.NewEvalUnaryNode(&tick.UnaryNode{
		Operator: tick.TokenNot,
		Node: &tick.BoolNode{
			Bool: false,
		},
	})

	if err != nil {
		t.Fatalf("Failed to compile unary node: %v", err)
	}

	result, err := evaluator.EvalString(tick.NewScope(), stateful.CreateExecutionState())

	expectedError := errors.New("expression returned unexpected type boolean")

	if err == nil && result != "" {
		t.Errorf("Expected an error, but got nil error and result: %v", result)
		return
	}

	if err.Error() != expectedError.Error() {
		t.Errorf("Got unexpected error:\ngot: %v\nexpected: %v\n", err, expectedError)
	}
}

func TestEvalUnaryNode_EvalInt64_FailedToEvaluateNode(t *testing.T) {
	evaluator, err := stateful.NewEvalUnaryNode(&tick.UnaryNode{
		Operator: tick.TokenMinus,
		Node: &tick.BoolNode{
			Bool: false,
		},
	})

	if err != nil {
		t.Fatalf("Failed to compile unary node: %v", err)
	}

	result, err := evaluator.EvalInt(tick.NewScope(), stateful.CreateExecutionState())

	expectedError := errors.New("expression returned unexpected type boolean")

	if err == nil && result != int64(0) {
		t.Errorf("Expected an error, but got nil error and result: %v", result)
		return
	}

	if err.Error() != expectedError.Error() {
		t.Errorf("Got unexpected error:\ngot: %v\nexpected: %v\n", err, expectedError)
	}
}

func TestEvalUnaryNode_EvalFloat64_FailedToEvaluateNode(t *testing.T) {
	evaluator, err := stateful.NewEvalUnaryNode(&tick.UnaryNode{
		Operator: tick.TokenMinus,
		Node: &tick.ReferenceNode{
			Reference: "value",
		},
	})

	if err != nil {
		t.Fatalf("Failed to compile unary node: %v", err)
	}

	result, err := evaluator.EvalFloat(tick.NewScope(), stateful.CreateExecutionState())

	expectedError := errors.New("name \"value\" is undefined. Names in scope:")

	if err == nil && result != float64(0) {
		t.Errorf("Expected an error, but got nil error and result: %v", result)
		return
	}

	if strings.TrimSpace(err.Error()) != expectedError.Error() {
		t.Errorf("Got unexpected error:\ngot: %v\nexpected: %v\n", err, expectedError)
	}
}
