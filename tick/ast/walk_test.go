package ast_test

import (
	"reflect"
	"testing"

	"github.com/influxdata/kapacitor/tick/ast"
)

func TestWalk(t *testing.T) {
	// AST for `var x = lambda: "value" < 10`
	root := &ast.ProgramNode{
		Nodes: []ast.Node{
			&ast.DeclarationNode{
				Left: &ast.IdentifierNode{
					Ident: "x",
				},
				Right: &ast.LambdaNode{
					Expression: &ast.BinaryNode{
						Operator: ast.TokenLess,
						Left: &ast.ReferenceNode{
							Reference: "value",
						},
						Right: &ast.NumberNode{
							IsInt: true,
							Base:  10,
							Int64: 10,
						},
					},
				},
			},
		},
	}

	expList := []string{
		"*ast.ProgramNode",
		"*ast.DeclarationNode",
		"*ast.IdentifierNode",
		"*ast.LambdaNode",
		"*ast.BinaryNode",
		"*ast.ReferenceNode",
		"*ast.NumberNode",
	}

	list := make([]string, 0, len(expList))
	f := func(n ast.Node) (ast.Node, error) {
		list = append(list, reflect.TypeOf(n).String())
		return n, nil
	}

	if _, err := ast.Walk(root, f); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(expList, list) {
		t.Errorf("unexpected walk list:\ngot\n%v\nexp\n%v\n", list, expList)
	}
}

func TestWalk_Mutate(t *testing.T) {
	// AST for `lambda: "value" < VAR_A AND "value" > VAR_B`
	root := &ast.LambdaNode{
		Expression: &ast.BinaryNode{
			Operator: ast.TokenAnd,
			Left: &ast.BinaryNode{
				Operator: ast.TokenLess,
				Left: &ast.ReferenceNode{
					Reference: "value",
				},
				Right: &ast.IdentifierNode{
					Ident: "VAR_A",
				},
			},
			Right: &ast.BinaryNode{
				Operator: ast.TokenGreater,
				Left: &ast.ReferenceNode{
					Reference: "value",
				},
				Right: &ast.IdentifierNode{
					Ident: "VAR_B",
				},
			},
		},
	}

	// Replace the IdentifierNodes with a NumberNode
	numbers := map[string]int64{
		"VAR_A": 42,
		"VAR_B": 3,
	}
	replace := func(n ast.Node) (ast.Node, error) {
		if ident, ok := n.(*ast.IdentifierNode); ok {
			return &ast.NumberNode{
				IsInt: true,
				Int64: numbers[ident.Ident],
				Base:  10,
			}, nil
		}
		return n, nil
	}

	if _, err := ast.Walk(root, replace); err != nil {
		t.Fatal(err)
	}

	expList := []string{
		"*ast.LambdaNode",
		"*ast.BinaryNode",
		"*ast.BinaryNode",
		"*ast.ReferenceNode",
		"*ast.NumberNode",
		"*ast.BinaryNode",
		"*ast.ReferenceNode",
		"*ast.NumberNode",
	}

	list := make([]string, 0, len(expList))
	f := func(n ast.Node) (ast.Node, error) {
		list = append(list, reflect.TypeOf(n).String())
		return n, nil
	}

	if _, err := ast.Walk(root, f); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(expList, list) {
		t.Errorf("unexpected walk list:\ngot\n%v\nexp\n%v\n", list, expList)
	}

	// Check the lambda formatted lambda expression
	expStr := `lambda: "value" < 42 AND "value" > 3`
	str := ast.Format(root)
	if expStr != str {
		t.Errorf("unexpected lambda str: got %s exp %s", str, expStr)
	}
}
