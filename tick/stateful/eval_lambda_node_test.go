package stateful_test

import (
	"regexp"
	"testing"
	"time"

	"github.com/influxdata/kapacitor/tick/ast"
	"github.com/influxdata/kapacitor/tick/stateful"
)

func TestEvalLambdaNode_EvalBool_Sanity(t *testing.T) {
	evaluator, err := stateful.NewEvalLambdaNode(&ast.LambdaNode{
		Expression: &ast.UnaryNode{
			Operator: ast.TokenNot,
			Node: &ast.BoolNode{
				Bool: false,
			},
		},
	})

	if err != nil {
		t.Fatalf("Failed to compile lambda node: %v", err)
	}

	result, err := evaluator.EvalBool(stateful.NewScope(), stateful.CreateExecutionState())
	if err != nil {
		t.Errorf("Got unexpected error: %v", err)
	}

	if !result {
		t.Errorf("unexpected result: got: %t, expected: true", result)
	}
}

func TestEvalLambdaNode_EvalFloat64_Sanity(t *testing.T) {
	evaluator, err := stateful.NewEvalLambdaNode(&ast.LambdaNode{
		Expression: &ast.UnaryNode{
			Operator: ast.TokenMinus,
			Node: &ast.NumberNode{
				IsFloat: true,
				Float64: float64(12),
			},
		},
	})

	if err != nil {
		t.Fatalf("Failed to compile lambda node: %v", err)
	}

	result, err := evaluator.EvalFloat(stateful.NewScope(), stateful.CreateExecutionState())
	if err != nil {
		t.Errorf("Got unexpected error: %v", err)
	}

	if result != float64(-12) {
		t.Errorf("unexpected result: got: %T(%v), expected: float64(-12)", result, result)
	}
}

func TestEvalLambdaNode_EvalInt64_Sanity(t *testing.T) {
	evaluator, err := stateful.NewEvalLambdaNode(&ast.LambdaNode{
		Expression: &ast.UnaryNode{
			Operator: ast.TokenMinus,
			Node: &ast.NumberNode{
				IsInt: true,
				Int64: int64(12),
			},
		},
	})

	if err != nil {
		t.Fatalf("Failed to compile lambda node: %v", err)
	}

	result, err := evaluator.EvalInt(stateful.NewScope(), stateful.CreateExecutionState())
	if err != nil {
		t.Errorf("Got unexpected error: %v", err)
	}

	if result != int64(-12) {
		t.Errorf("unexpected result: got: %T(%v), expected: int64(-12)", result, result)
	}
}

func TestEvalLambdaNode_EvalDuration_Sanity(t *testing.T) {
	evaluator, err := stateful.NewEvalLambdaNode(&ast.LambdaNode{
		Expression: &ast.UnaryNode{
			Operator: ast.TokenMinus,
			Node: &ast.DurationNode{
				Dur: time.Minute,
			},
		},
	})

	if err != nil {
		t.Fatalf("Failed to compile lambda node: %v", err)
	}

	result, err := evaluator.EvalDuration(stateful.NewScope(), stateful.CreateExecutionState())
	if err != nil {
		t.Errorf("Got unexpected error: %v", err)
	}

	if got, exp := result, -time.Minute; got != exp {
		t.Errorf("unexpected result: got: %T(%v), expected: %s", got, got, exp)
	}
}

func TestEvalLambdaNode_EvalString_Sanity(t *testing.T) {
	evaluator, err := stateful.NewEvalLambdaNode(&ast.LambdaNode{
		Expression: &ast.StringNode{
			Literal: "string",
		},
	})

	if err != nil {
		t.Fatalf("Failed to compile lambda node: %v", err)
	}

	result, err := evaluator.EvalString(stateful.NewScope(), stateful.CreateExecutionState())
	if err != nil {
		t.Errorf("Got unexpected error: %v", err)
	}

	if got, exp := result, "string"; got != exp {
		t.Errorf("unexpected result: got: %T(%v), expected: %s", got, got, exp)
	}
}

func TestEvalLambdaNode_EvalRegex_Sanity(t *testing.T) {
	evaluator, err := stateful.NewEvalLambdaNode(&ast.LambdaNode{
		Expression: &ast.RegexNode{
			Regex: regexp.MustCompile("^abc.*"),
		},
	})

	if err != nil {
		t.Fatalf("Failed to compile lambda node: %v", err)
	}

	result, err := evaluator.EvalRegex(stateful.NewScope(), stateful.CreateExecutionState())
	if err != nil {
		t.Errorf("Got unexpected error: %v", err)
	}

	if got, exp := result.String(), "^abc.*"; exp != got {
		t.Errorf("unexpected result: got: %T(%v), expected: %s", got, got, exp)
	}
}

func TestEvalLambdaNode_EvalBool_SeparateState(t *testing.T) {
	// l1 = lambda: count() > 5
	// l2 = lambda: count() > 10
	// Because of short-circuiting it should take 15 calls before,
	// 'l1 AND l2' evaluates to true.

	l1 := &ast.LambdaNode{
		Expression: &ast.BinaryNode{
			Operator: ast.TokenGreater,
			Left: &ast.FunctionNode{
				Func: "count",
			},
			Right: &ast.NumberNode{
				IsInt: true,
				Int64: 5,
			},
		},
	}

	l2 := &ast.LambdaNode{
		Expression: &ast.BinaryNode{
			Operator: ast.TokenGreater,
			Left: &ast.FunctionNode{
				Func: "count",
			},
			Right: &ast.NumberNode{
				IsInt: true,
				Int64: 10,
			},
		},
	}

	evaluator, err := stateful.NewEvalBinaryNode(&ast.BinaryNode{
		Operator: ast.TokenAnd,
		Left:     l1,
		Right:    l2,
	})

	if err != nil {
		t.Fatalf("Failed to compile lambda node: %v", err)
	}

	count := 15
	for i := 0; i < count; i++ {
		result, err := evaluator.EvalBool(stateful.NewScope(), stateful.CreateExecutionState())
		if err != nil {
			t.Fatalf("Got unexpected error: %v", err)
		}

		if got, exp := result, false; got != exp {
			t.Fatalf("unexpected result: got: %T(%v), expected: %t", got, got, exp)
		}
	}
	// Final time it should be true
	result, err := evaluator.EvalBool(stateful.NewScope(), stateful.CreateExecutionState())
	if err != nil {
		t.Fatalf("Got unexpected error: %v", err)
	}

	if got, exp := result, true; got != exp {
		t.Fatalf("unexpected result: got: %T(%v), expected: %t", got, got, exp)
	}
}
