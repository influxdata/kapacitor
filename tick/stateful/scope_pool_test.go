package stateful_test

import (
	"reflect"
	"testing"

	"github.com/influxdata/kapacitor/tick/ast"
	"github.com/influxdata/kapacitor/tick/stateful"
)

func TestScopePool_Sanity(t *testing.T) {
	n := stateful.NewScopePool([]string{"value"})

	// first
	scope := n.Get()

	_, existsErr := scope.Get("value")

	if existsErr != nil {
		t.Errorf("First: Expected \"value\" to exist in the scope, but go an error: %v", existsErr)
	}

	// second, after put
	n.Put(scope)

	scope = n.Get()
	_, existsErr = scope.Get("value")

	if existsErr != nil {
		t.Errorf("Second: Expected \"value\" to exist in the scope, but go an error: %v", existsErr)
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
