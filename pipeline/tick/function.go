package tick

import (
	"fmt"

	"github.com/influxdata/kapacitor/tick/ast"
)

// Function builds functions from pipeline nodes
type Function struct {
	Parents []ast.Node
	err     error
	prev    ast.Node
}

// Pipe produces an ast.FunctionNode within a Pipe Chain.  May return
// the parent node if all args evaluate to the zero value.
// Assumes there is only one Pipe called per Function.
// Assumes one parent exists.
func (f *Function) Pipe(name string, args ...interface{}) *Function {
	if f.err != nil {
		return f
	}

	if len(f.Parents) == 0 {
		f.err = fmt.Errorf("Parent required for function creation")
		return f
	}

	fn, err := Func(name, args...)
	if err != nil {
		f.err = err
		return f
	}

	// The function contains all zero values, so we don't need to add it to the output
	if fn != nil {
		f.prev = Pipe(f.Parents[0], fn)
	}
	return f
}

// At produces an ast.FunctionNode within an At Chain.  May return
// the parent node if all args evaluate to the zero value.
// Assumes there is only one At called per Function.
// Assumes one parent exists.
func (f *Function) At(name string, args ...interface{}) *Function {
	if f.err != nil {
		return f
	}

	if len(f.Parents) == 0 {
		f.err = fmt.Errorf("Parent required for function creation")
		return f
	}

	fn, err := Func(name, args...)
	if err != nil {
		f.err = err
		return f
	}

	// The function contains all zero values, so we don't need to add it to the output
	if fn != nil {
		f.prev = At(f.Parents[0], fn)
	}
	return f
}

// Dot produces an ast.FunctionNode within a Dot Chain.  May return
// a parent node if all args evaluate to the zero value
// Assumes a previous node has been created.
func (f *Function) Dot(name string, args ...interface{}) *Function {
	if f.err != nil {
		return f
	}

	fn, err := Func(name, args...)
	if err != nil {
		f.err = err
		return f
	}

	// The function contains all zero values, so we don't need to add it to the output
	if fn != nil {
		f.prev = Dot(f.prev, fn)
	}
	return f
}

// DotZeroValueOK produces an ast.FunctionNode within a Dot Chain.
// Assumes a previous node has been created.
func (f *Function) DotZeroValueOK(name string, args ...interface{}) *Function {
	if f.err != nil {
		return f
	}

	fn, err := FuncWithZero(name, args...)
	if err != nil {
		f.err = err
		return f
	}

	f.prev = Dot(f.prev, fn)
	return f
}

// DotRemoveZeroValue produces an ast.FunctionNode within a Dot Chain.
// Assumes a previous node has been created. Remove all zero argument values from func.
func (f *Function) DotRemoveZeroValue(name string, args ...interface{}) *Function {
	if f.err != nil {
		return f
	}

	fn, err := FuncRemoveZero(name, args...)
	if err != nil {
		f.err = err
		return f
	}

	f.prev = Dot(f.prev, fn)
	return f
}

// DotIf produces an ast.FunctionNode within a Dot Chain if use is true
// Assumes a previous node has been created.
func (f *Function) DotIf(name string, use bool) *Function {
	if f.err != nil {
		return f
	}

	if !use {
		return f
	}

	fn, err := Func(name)
	if err != nil {
		f.err = err
		return f
	}

	f.prev = Dot(f.prev, fn)
	return f
}

// DotNotNil produces an ast.FunctionNode within a Dot Chain if arg is not nil.
// Assumes a previous node has been created.
func (f *Function) DotNotNil(name string, arg interface{}) *Function {
	if f.err != nil {
		return f
	}

	if arg == nil {
		return f
	}

	fn, err := Func(name, arg)
	if err != nil {
		f.err = err
		return f
	}

	f.prev = Dot(f.prev, fn)
	return f
}

// DotNotEmpty produces an ast.FunctionNode within a Dot Chain if the argument list is not empty.
// Assumes a previous node has been created.
func (f *Function) DotNotEmpty(name string, arg ...interface{}) *Function {
	if f.err != nil {
		return f
	}

	if len(arg) == 0 {
		return f
	}

	fn, err := Func(name, arg...)
	if err != nil {
		f.err = err
		return f
	}

	f.prev = Dot(f.prev, fn)
	return f
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

// Func produces an ast.FunctionNode. Can return a nil Node
// if all function arguments evaluate to the zero value.
func Func(name string, args ...interface{}) (ast.Node, error) {
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

// FuncWithZero produces an ast.FunctionNode.
// All function arguments that evaluate to the zero value are kept.
func FuncWithZero(name string, args ...interface{}) (ast.Node, error) {
	if len(args) == 0 {
		return &ast.FunctionNode{
			Func: name,
		}, nil
	}

	astArgs := []ast.Node{}
	for _, arg := range args {
		lit, err := Literal(arg)
		if err != nil {
			return nil, err
		}
		astArgs = append(astArgs, lit)
	}

	return &ast.FunctionNode{
		Func: name,
		Args: astArgs,
	}, nil
}

// FuncRemoveZero produces an ast.FunctionNode.
// All function arguments that evaluate to the zero value are removed from func.
func FuncRemoveZero(name string, args ...interface{}) (ast.Node, error) {
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

	return &ast.FunctionNode{
		Func: name,
		Args: astArgs,
	}, nil
}

// IsZero check if the argument is the tickscript zero value for that type.
func IsZero(arg interface{}) bool {
	typeOf := ast.TypeOf(arg)
	if typeOf == ast.TList {
		return len(arg.([]interface{})) == 0
	}
	return arg == ast.ZeroValue(typeOf)
}

// Literal produces an ast Literal (NumberNode, etc).
func Literal(lit interface{}) (ast.Node, error) {
	typeOf := ast.TypeOf(lit)
	if typeOf == ast.InvalidType {
		if node, ok := lit.(ast.Node); ok {
			return node, nil
		}
		return nil, fmt.Errorf("unsupported literal type %T", lit)
	}
	return ast.ValueToLiteralNode(&NullPosition{}, lit)
}

func args(a []string) []interface{} {
	r := make([]interface{}, len(a))
	for i := range a {
		r[i] = a[i]
	}
	return r
}

func largs(a []*ast.LambdaNode) []interface{} {
	r := make([]interface{}, len(a))
	for i := range a {
		r[i] = a[i]
	}
	return r
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
