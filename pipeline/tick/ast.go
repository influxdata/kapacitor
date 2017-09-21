package tick

import (
	"bytes"

	"github.com/influxdata/kapacitor/tick/ast"
)

// AST converts a pipeline into an AST
type AST struct {
	Node ast.Node
}

// TICKScript produces a TICKScript from the AST
func (a *AST) TICKScript() string {
	var buf bytes.Buffer
	a.Node.Format(&buf, "", false)
	return buf.String()
}

// PipeFunction produces an ast.FunctionNode within a Pipe Chain.  May return
// the left node if all args evaluate to the zero value
func PipeFunction(left ast.Node, name string, args ...interface{}) (ast.Node, error) {
	fn, err := Function(name, args...)
	if err != nil {
		return nil, err
	}

	// The function contains all zero values, so we don't need to add it to the output
	if fn == nil {
		return left, nil
	}
	return Pipe(left, fn), nil
}

// DotFunction produces an ast.FunctionNode within a Dot Chain.  May return
// a nil node if all args evaluate to the zero value
func DotFunction(left ast.Node, name string, args ...interface{}) (ast.Node, error) {
	fn, err := Function(name, args...)
	if err != nil {
		return nil, err
	}

	// The function contains all zero values, so we don't need to add it to the output
	if fn == nil {
		return left, nil
	}
	return Dot(left, fn), nil
}

// DotFunctionIf produces an ast.FunctionNode within a Dot Chain if use is true
func DotFunctionIf(left ast.Node, name string, use bool) (ast.Node, error) {
	if !use {
		return left, nil
	}

	fn, err := Function(name)
	if err != nil {
		return nil, err
	}

	return Dot(left, fn), nil
}

// Pipe produces a Pipe ast.ChainNode
func Pipe(left, right ast.Node) ast.Node {
	return &ast.ChainNode{
		Operator: ast.TokenPipe,
		Left:     left,
		Right:    right,
	}
}

// At produces an At ast.ChainNode
func At(left, right ast.Node) ast.Node {
	return &ast.ChainNode{
		Operator: ast.TokenAt,
		Left:     left,
		Right:    right,
	}
}

// Dot produces a Defense of the ancients.ChainNode
func Dot(left, right ast.Node) ast.Node {
	return &ast.ChainNode{
		Operator: ast.TokenDot,
		Left:     left,
		Right:    right,
	}
}

// Function produces an ast.FunctionNode. Can return a nil Node
// if all function arguments evaluate to the zero value.
func Function(name string, args ...interface{}) (ast.Node, error) {
	if len(args) == 0 {
		return &ast.FunctionNode{
			Func: name,
		}, nil
	}

	astArgs := []ast.Node{}
	for _, arg := range args {
		// Skip zero values as they don't need to be rendered
		if IsZero(arg) {
			continue
		}

		lit, err := Literal(arg)
		if err != nil {
			return nil, err
		}
		astArgs = append(astArgs, lit)
	}

	// Because all args are zero-valued, the node isn't needed,
	// so, return a nil to signify empty node.
	if len(astArgs) == 0 {
		return nil, nil
	}
	return &ast.FunctionNode{
		Func: name,
		Args: astArgs,
	}, nil
}

func IsZero(arg interface{}) bool {
	typeOf := ast.TypeOf(arg)
	if typeOf == ast.TList {
		return len(arg.([]interface{})) == 0
	}
	return arg == ast.ZeroValue(typeOf)
}

// Literal produces an ast Literal (NumberNode, etc).
func Literal(lit interface{}) (ast.Node, error) {
	return ast.ValueToLiteralNode(&NullPosition{}, lit)
}

var _ ast.Position = &NullPosition{}

// NullPosition is a NOOP to satisfy the tick AST package
type NullPosition struct{}

// Position returns 0
func (n *NullPosition) Position() int { return 0 }

// Line returns 0
func (n *NullPosition) Line() int { return 0 }

// Char returns 0
func (n *NullPosition) Char() int { return 0 }
