package stateful_test

import (
	"reflect"
	"testing"

	"github.com/influxdata/kapacitor/tick/ast"
	"github.com/influxdata/kapacitor/tick/stateful"
)

func TestScopePool_Sanity(t *testing.T) {
	n := stateful.NewScopePool([]string{"value"})

	scope := n.Get()

	if scope.Has("value") {
		t.Errorf("First: expected scope to not have a value set")
	}
	value := 42
	scope.Set("value", value)
	if !scope.Has("value") {
		t.Errorf("First: expected scope to have a value set")
	}
	if v, err := scope.Get("value"); err != nil || v != value {
		t.Errorf("First: unexpected scope value got %v exp %v", v, value)
	}

	n.Put(scope)

	// Scope should be empty now
	scope = n.Get()
	if scope.Has("value") {
		t.Errorf("Second: expected scope to not have a value set")
	}
	value = 24
	scope.Set("value", value)
	if !scope.Has("value") {
		t.Errorf("Second: expected scope to have a value set")
	}
	if v, err := scope.Get("value"); err != nil || v != value {
		t.Errorf("Second: unexpected scope value got %v exp %v", v, value)
	}
}

func TestExpression_RefernceVariables(t *testing.T) {

	type expectation struct {
		node         ast.Node
		refVariables []string
	}

	expectations := []expectation{
		{node: &ast.NumberNode{IsFloat: true}, refVariables: make([]string, 0)},
		{node: &ast.BoolNode{}, refVariables: make([]string, 0)},

		{node: &ast.ReferenceNode{Reference: "yosi"}, refVariables: []string{"yosi"}},
		{node: &ast.BinaryNode{Left: &ast.ReferenceNode{Reference: "value"}, Right: &ast.NumberNode{IsInt: true}}, refVariables: []string{"value"}},
	}

	for i, expect := range expectations {
		refVariables := stateful.FindReferenceVariables(expect.node)
		if !reflect.DeepEqual(refVariables, expect.refVariables) {
			t.Errorf("[Iteration: %v, Node: %T] Got unexpected result:\ngot: %v\nexpected: %v", i+1, expect.node, refVariables, expect.refVariables)
		}

	}
}
