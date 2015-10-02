package expr

import (
	"fmt"
	"strconv"
)

// A callable function from within the expression
type Func func(...float64) (float64, error)

// Lookup for variables
type Vars map[string]float64

// Lookup for functions
type Funcs map[string]Func

type ReturnType int

const (
	ReturnBool ReturnType = iota
	ReturnNumber
)

func (r ReturnType) String() string {
	switch r {
	case ReturnBool:
		return "bool"
	case ReturnNumber:
		return "number"
	}
	return "unknown"
}

func (r ReturnType) RType() ReturnType {
	return r
}

type node interface {
	Type() nodeType
	String() string
	Position() int                           // byte position of start of node in full original input string
	Check() error                            // performs type checking for itself and sub-nodes
	RType() ReturnType                       // the return type of the node
	Return(v Vars, f Funcs) (float64, error) // the return value of the node
}

// nodeType identifies the type of a parse tree node.
type nodeType int

func (t nodeType) Type() nodeType {
	return t
}

const (
	nodeBinary nodeType = iota // Binary operator: math
	nodeUnary                  // Unary operator: -
	nodeNumber                 // A numerical constant.
	nodeVar                    // An variable
	nodeFunc                   // A function call node
)

type pos int

func (p pos) Position() int {
	return int(p)
}

func boolToFloat(b bool) float64 {
	if b {
		return 1
	}
	return -1
}

// numberNode holds a number: signed or unsigned integer or float.
// The value is parsed and stored under all the types that can represent the value.
// This simulates in a small amount of code the behavior of Go's ideal constants.
type numberNode struct {
	nodeType
	ReturnType
	pos
	Float64 float64 // The floating-point value.
	Text    string  // The original textual representation from the input.
}

// create a new number from a text string
func newNumber(p int, text string) (*numberNode, error) {
	n := &numberNode{
		nodeType:   nodeNumber,
		ReturnType: ReturnNumber,
		pos:        pos(p),
		Text:       text,
	}
	// Only parse uints
	f, err := strconv.ParseFloat(text, 64)
	if err == nil {
		n.Float64 = f
	}
	return n, nil
}

func (n *numberNode) String() string {
	return fmt.Sprintf("numberNode{%s}", n.Text)
}

func (n *numberNode) Check() error {
	return nil
}

func (n *numberNode) Return(v Vars, f Funcs) (float64, error) {
	return n.Float64, nil
}

//Holds the textual representation of an variable
type varNode struct {
	nodeType
	ReturnType
	pos
	Var string // The variable name
}

func newVar(p int, v string) *varNode {
	return &varNode{
		nodeType:   nodeVar,
		ReturnType: ReturnNumber,
		pos:        pos(p),
		Var:        v,
	}
}

func (v *varNode) String() string {
	return fmt.Sprintf("varNode{%s}", v.Var)
}

func (v *varNode) Check() error {
	return nil
}

func (v *varNode) Return(vs Vars, fs Funcs) (float64, error) {
	f, ok := vs[v.Var]
	if !ok {
		return -1, fmt.Errorf("undefined vairable %q", v.Var)
	}
	return f, nil
}

// unaryNode holds two arguments and an operator.
type unaryNode struct {
	nodeType
	pos
	n        node
	Operator token
}

func newUnary(operator token, n node) *unaryNode {
	return &unaryNode{
		nodeType: nodeUnary,
		pos:      pos(operator.pos),
		n:        n,
		Operator: operator,
	}
}

func (u *unaryNode) String() string {
	return fmt.Sprintf("unaryNode{%s %s}", u.Operator.val, u.n)
}

func (u *unaryNode) Check() error {
	if u.Operator.val != "-" {
		return fmt.Errorf("unknown unary operator %q", u.Operator.val)
	}
	err := u.n.Check()
	return err
}
func (u *unaryNode) RType() ReturnType {
	switch u.Operator.val {
	case "-":
		return ReturnNumber
	case "!":
		return ReturnBool
	default:
		panic("invalid unary operator")
	}
}

func (u *unaryNode) Return(v Vars, f Funcs) (float64, error) {
	r, err := u.n.Return(v, f)
	if err != nil {
		return 0, err
	}
	return -1 * r, nil
}

// binaryNode holds two arguments and an operator.
type binaryNode struct {
	nodeType
	pos
	Left     node
	Right    node
	Operator token
}

func newBinary(operator token, left, right node) *binaryNode {
	return &binaryNode{
		nodeType: nodeBinary,
		pos:      pos(operator.pos),
		Left:     left,
		Right:    right,
		Operator: operator,
	}
}

func (b *binaryNode) String() string {
	return fmt.Sprintf("binaryNode{%s %s %s}", b.Left, b.Operator.val, b.Right)
}

func (b *binaryNode) Check() error {
	err := b.Left.Check()
	if err != nil {
		return err
	}
	err = b.Right.Check()
	if err != nil {
		return err
	}

	lt := b.Left.RType()
	rt := b.Right.RType()
	switch b.Operator.val {
	case "+", "-", "*", "/", ">", ">=", "<", "<=":
		if lt != ReturnNumber || rt != ReturnNumber {
			return fmt.Errorf("operator %q requires numeric operands.", b.Operator.val)
		}
	case "&&", "||":
		if lt != ReturnBool || rt != ReturnBool {
			return fmt.Errorf("operator %q requires logical operands.", b.Operator.val)
		}
	default:
		return fmt.Errorf("unknown binary operator %q", b.Operator.val)
	}
	return nil
}

func (b *binaryNode) RType() ReturnType {
	switch b.Operator.val {
	case "+", "-", "*", "/":
		return ReturnNumber
	case ">", ">=", "<", "<=", "==", "!=", "&&", "||":
		return ReturnBool
	default:
		panic("invalid binary operator")
	}
}

func (b *binaryNode) Return(vs Vars, f Funcs) (v float64, err error) {
	var l, r float64
	l, err = b.Left.Return(vs, f)
	if err != nil {
		return
	}
	r, err = b.Right.Return(vs, f)
	if err != nil {
		return
	}
	switch b.Operator.val {
	case "+":
		v = l + r
	case "-":
		v = l - r
	case "*":
		v = l * r
	case "/":
		v = l / r
	case ">":
		v = boolToFloat(l > r)
	case ">=":
		v = boolToFloat(l >= r)
	case "<":
		v = boolToFloat(l < r)
	case "<=":
		v = boolToFloat(l <= r)
	case "==":
		v = boolToFloat(l == r)
	case "!=":
		v = boolToFloat(l != r)
	case "&&":
		v = boolToFloat(l > 0 && r > 0)
	case "||":
		v = boolToFloat(l > 0 || r > 0)
	default:
		return -1, fmt.Errorf("return: unknown operator %s", b.Operator.val)
	}
	return
}

// funcNode holds a list of arguments and a function to call
type funcNode struct {
	nodeType
	ReturnType
	pos
	name string
	args []node
}

func newFunc(p int, name string, args []node) *funcNode {
	return &funcNode{
		nodeType:   nodeFunc,
		ReturnType: ReturnNumber,
		pos:        pos(p),
		name:       name,
		args:       args,
	}
}

func (f *funcNode) String() string {
	return fmt.Sprintf("funcNode{%s %s}", f.name, f.args)
}

func (f *funcNode) Check() error {
	for _, a := range f.args {
		err := a.Check()
		if err != nil {
			return err
		}
	}
	return nil
}

func (f *funcNode) Return(v Vars, fs Funcs) (float64, error) {
	fnc, ok := fs[f.name]
	if !ok {
		return 0, fmt.Errorf("undefined function %q", f.name)
	}

	args := make([]float64, len(f.args))
	for i, a := range f.args {
		r, err := a.Return(v, fs)
		if err != nil {
			return 0, err
		}
		args[i] = r
	}
	r, err := fnc(args...)
	fmt.Println("FR:", f.name, args, r)
	return r, err
}
