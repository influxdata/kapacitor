package expr

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
)

// Lookup for variables
type Vars map[string]interface{}

type ReturnType int

const (
	ReturnUnknown ReturnType = iota
	ReturnBool
	ReturnNum
	ReturnStr
)

func (r ReturnType) String() string {
	switch r {
	case ReturnBool:
		return "boolean"
	case ReturnNum:
		return "number"
	case ReturnStr:
		return "string"
	}
	return "unknown"
}

func (r ReturnType) RType() ReturnType {
	return r
}

type node interface {
	String() string
	Position() int                              // byte position of start of node in full original input string
	Check() error                               // performs type checking for itself and sub-nodes
	RType() ReturnType                          // the return type of the node
	ReturnNum(v Vars, f Funcs) (float64, error) // the numeric return value of the node
	ReturnBool(v Vars, f Funcs) (bool, error)   // the boolean return value of the node
	ReturnStr(v Vars, f Funcs) (string, error)  // the string return value of the node
}

type pos int

func (p pos) Position() int {
	return int(p)
}

// This error is thrown when the wrong Return* method is called on a node
var errWrongRType = errors.New("wrong return type")

// numberNode holds a number: signed or unsigned integer or float.
// The value is parsed and stored under all the types that can represent the value.
// This simulates in a small amount of code the behavior of Go's ideal constants.
type numNode struct {
	ReturnType
	pos
	Float64 float64 // The floating-point value.
	Text    string  // The original textual representation from the input.
}

// create a new number from a text string
func newNum(p int, text string) (*numNode, error) {
	n := &numNode{
		ReturnType: ReturnNum,
		pos:        pos(p),
		Text:       text,
	}
	f, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return nil, err
	}
	n.Float64 = f
	return n, nil
}

func (n *numNode) String() string {
	return fmt.Sprintf("numberNode{%s}", n.Text)
}

func (n *numNode) Check() error {
	return nil
}

func (n *numNode) ReturnStr(v Vars, f Funcs) (string, error) {
	return "", errWrongRType
}

func (n *numNode) ReturnBool(v Vars, f Funcs) (bool, error) {
	return false, errWrongRType
}

func (n *numNode) ReturnNum(v Vars, f Funcs) (float64, error) {
	return n.Float64, nil
}

// boolNode: holds a boolean literal
type boolNode struct {
	ReturnType
	pos
	Bool bool
	Text string // The original textual representation from the input.
}

// create a new number from a text string
func newBool(p int, text string) (*boolNode, error) {
	n := &boolNode{
		ReturnType: ReturnBool,
		pos:        pos(p),
		Text:       text,
	}
	b, err := strconv.ParseBool(text)
	if err != nil {
		return nil, err
	}
	n.Bool = b
	return n, nil
}

func (n *boolNode) String() string {
	return fmt.Sprintf("boolNode{%s}", n.Text)
}

func (n *boolNode) Check() error {
	return nil
}

func (n *boolNode) ReturnStr(v Vars, f Funcs) (string, error) {
	return "", errWrongRType
}

func (n *boolNode) ReturnBool(v Vars, f Funcs) (bool, error) {
	return n.Bool, nil
}

func (n *boolNode) ReturnNum(v Vars, f Funcs) (float64, error) {
	return 0, errWrongRType
}

//Holds the textual representation of an variable
type varNode struct {
	ReturnType
	pos
	Var string // The variable name
}

func newVar(p int, v string) *varNode {
	if v[0] == '"' {
		v = v[1 : len(v)-1]
	}
	return &varNode{
		ReturnType: ReturnUnknown,
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

func (v *varNode) ReturnStr(vs Vars, fs Funcs) (string, error) {
	f, ok := vs[v.Var]
	if !ok {
		return "", fmt.Errorf("undefined vairable %q", v.Var)
	}
	s, ok := f.(string)
	if !ok {
		return "", fmt.Errorf("undefined vairable %q", v.Var)
	}
	return s, nil
}
func (v *varNode) ReturnBool(vs Vars, fs Funcs) (bool, error) {
	f, ok := vs[v.Var]
	if !ok {
		return false, fmt.Errorf("undefined vairable %q", v.Var)
	}
	b, ok := f.(bool)
	if !ok {
		return false, fmt.Errorf("undefined vairable %q", v.Var)
	}
	return b, nil
}
func (v *varNode) ReturnNum(vs Vars, fs Funcs) (float64, error) {
	f, ok := vs[v.Var]
	if !ok {
		return -1, fmt.Errorf("undefined vairable %q", v.Var)
	}
	num, ok := f.(float64)
	if !ok {
		return -1, fmt.Errorf("undefined vairable %q", v.Var)
	}
	return num, nil
}

//Holds the textual representation of a string literal
type strNode struct {
	ReturnType
	pos
	Literal string
}

func newStr(p int, s string) *strNode {

	buf := make([]byte, 0, len(s))
	for i := 1; i < len(s)-1; i++ {
		if s[i] == '\\' && s[i+1] == '\'' {
			i++
		}
		buf = append(buf, s[i])
	}
	return &strNode{
		ReturnType: ReturnStr,
		pos:        pos(p),
		Literal:    string(buf),
	}
}

func (s *strNode) String() string {
	return fmt.Sprintf("stringNode{%s}", s.Literal)
}

func (s *strNode) Check() error {
	return nil
}

func (s *strNode) ReturnBool(vs Vars, fs Funcs) (bool, error) {
	return false, errWrongRType
}
func (s *strNode) ReturnNum(vs Vars, fs Funcs) (float64, error) {
	return 0, errWrongRType
}
func (s *strNode) ReturnStr(vs Vars, fs Funcs) (string, error) {
	return s.Literal, nil
}

//Holds the textual representation of a regex literal
type regexNode struct {
	ReturnType
	pos
	Literal string
}

func newRegex(p int, s string) *regexNode {

	buf := make([]byte, 0, len(s))
	for i := 1; i < len(s)-1; i++ {
		if s[i] == '\\' && s[i+1] == '/' {
			i++
		}
		buf = append(buf, s[i])
	}
	return &regexNode{
		ReturnType: ReturnStr,
		pos:        pos(p),
		Literal:    string(buf),
	}
}

func (s *regexNode) String() string {
	return fmt.Sprintf("regexNode{%s}", s.Literal)
}

func (s *regexNode) Check() error {
	return nil
}

func (s *regexNode) ReturnBool(vs Vars, fs Funcs) (bool, error) {
	return false, errWrongRType
}
func (s *regexNode) ReturnNum(vs Vars, fs Funcs) (float64, error) {
	return 0, errWrongRType
}
func (s *regexNode) ReturnStr(vs Vars, fs Funcs) (string, error) {
	return s.Literal, nil
}

// unaryNode holds two arguments and an operator.
type unaryNode struct {
	pos
	n        node
	Operator tokenType
}

func newUnary(operator token, n node) *unaryNode {
	return &unaryNode{
		pos:      pos(operator.pos),
		n:        n,
		Operator: operator.typ,
	}
}

func (u *unaryNode) String() string {
	return fmt.Sprintf("unaryNode{%s %s}", u.Operator, u.n)
}

func (u *unaryNode) Check() error {
	if u.Operator != tokenMinus && u.Operator != tokenNot {
		return fmt.Errorf("unknown unary operator %q", u.Operator)
	}
	err := u.n.Check()
	return err
}
func (u *unaryNode) RType() ReturnType {
	switch u.Operator {
	case tokenMinus:
		return ReturnNum
	case tokenNot:
		return ReturnBool
	default:
		panic("invalid unary operator")
	}
}

func (u *unaryNode) ReturnBool(v Vars, f Funcs) (bool, error) {
	r, err := u.n.ReturnBool(v, f)
	if err != nil {
		return false, err
	}
	return !r, nil
}

func (u *unaryNode) ReturnStr(v Vars, f Funcs) (string, error) {
	return "", errWrongRType
}

func (u *unaryNode) ReturnNum(v Vars, f Funcs) (float64, error) {
	r, err := u.n.ReturnNum(v, f)
	if err != nil {
		return 0, err
	}
	return -1 * r, nil
}

// binaryNode holds two arguments and an operator.
type binaryNode struct {
	pos
	Left     node
	Right    node
	Operator tokenType
}

func newBinary(operator token, left, right node) *binaryNode {
	return &binaryNode{
		pos:      pos(operator.pos),
		Left:     left,
		Right:    right,
		Operator: operator.typ,
	}
}

func (b *binaryNode) String() string {
	return fmt.Sprintf("binaryNode{%s %s %s}", b.Left, b.Operator, b.Right)
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
	if lt == ReturnUnknown || rt == ReturnUnknown {
		return nil
	}
	switch o := b.Operator; {
	case isMathOperator(o):
		if lt != ReturnNum || rt != ReturnNum {
			return fmt.Errorf("operator %q requires numeric operands.", b.Operator)
		}
	case isCompOperator(o):
		if lt != rt {
			return fmt.Errorf("operator %q requires operands of same type.", b.Operator)
		}
	case o == tokenAnd, o == tokenOr:
		if lt != ReturnBool || rt != ReturnBool {
			return fmt.Errorf("operator %q requires logical operands.", b.Operator)
		}
	default:
		return fmt.Errorf("unknown binary operator %q", b.Operator)
	}
	return nil
}

func (b *binaryNode) RType() ReturnType {
	switch o := b.Operator; {
	case isMathOperator(o):
		return ReturnNum
	case isCompOperator(o), o == tokenAnd, o == tokenOr:
		return ReturnBool
	default:
		panic("invalid binary operator")
	}
}

func (b *binaryNode) ReturnStr(vs Vars, f Funcs) (v string, err error) {
	return "", errWrongRType
}

func (b *binaryNode) ReturnNum(vs Vars, f Funcs) (v float64, err error) {
	var l, r float64
	l, err = b.Left.ReturnNum(vs, f)
	if err != nil {
		return
	}
	r, err = b.Right.ReturnNum(vs, f)
	if err != nil {
		return
	}
	switch b.Operator {
	case tokenPlus:
		v = l + r
	case tokenMinus:
		v = l - r
	case tokenMult:
		v = l * r
	case tokenDiv:
		v = l / r
	default:
		return -1, fmt.Errorf("return: unknown operator %s", b.Operator)
	}
	return
}

func (b *binaryNode) ReturnBool(vs Vars, f Funcs) (v bool, err error) {
	switch o := b.Operator; {
	case isCompOperator(o):
		rt := b.Left.RType()
		if rt == ReturnUnknown {
			rt = b.Right.RType()
		}
		// try and figure out the type
		if rt == ReturnUnknown {
			_, err := b.Left.ReturnStr(vs, f)
			if err == nil {
				rt = ReturnStr
			} else {
				_, err := b.Left.ReturnNum(vs, f)
				if err == nil {
					rt = ReturnNum
				} else {
					return false, fmt.Errorf("cannot determine type of operands %q %q", b.Left, b.Right)
				}
			}
		}
		if rt == ReturnNum {
			var l, r float64
			l, err = b.Left.ReturnNum(vs, f)
			if err != nil {
				return
			}
			r, err = b.Right.ReturnNum(vs, f)
			if err != nil {
				return
			}
			return compareNums(l, r, o)
		} else if rt == ReturnStr {
			var l, r string
			l, err = b.Left.ReturnStr(vs, f)
			if err != nil {
				return
			}
			r, err = b.Right.ReturnStr(vs, f)
			if err != nil {
				return
			}
			return compareStrs(l, r, o)
		} else {
			return false, fmt.Errorf("invalid operands to %v, expected strings or numbers", b.Operator)
		}
	case o == tokenAnd, o == tokenOr:
		var l, r bool
		l, err = b.Left.ReturnBool(vs, f)
		if err != nil {
			return
		}
		r, err = b.Right.ReturnBool(vs, f)
		if err != nil {
			return
		}
		if o == tokenAnd {
			v = l && r
		} else {
			v = l || r
		}
	default:
		return false, fmt.Errorf("return: unknown operator %v", b.Operator)
	}
	return
}

func compareStrs(l, r string, o tokenType) (b bool, err error) {
	switch o {
	case tokenLess:
		b = l < r
	case tokenLessEqual:
		b = l <= r
	case tokenGreater:
		b = l > r
	case tokenGreaterEqual:
		b = l >= r
	case tokenEqual:
		b = l == r
	case tokenNotEqual:
		b = l != r
	case tokenRegexEqual:
		b, err = regexp.MatchString(l, r)
	case tokenRegexNotEqual:
		b, err = regexp.MatchString(l, r)
	default:
		err = fmt.Errorf("unsupported operator %v on strings", o)
	}
	return
}

func compareNums(l, r float64, o tokenType) (b bool, err error) {
	switch o {
	case tokenLess:
		b = l < r
	case tokenLessEqual:
		b = l <= r
	case tokenGreater:
		b = l > r
	case tokenGreaterEqual:
		b = l >= r
	case tokenEqual:
		b = l == r
	case tokenNotEqual:
		b = l != r
	default:
		err = fmt.Errorf("unsupported operator %v on numbers", o)
	}
	return
}

// funcNode holds a list of arguments and a function to call
type funcNode struct {
	ReturnType
	pos
	name string
	args []node
}

func newFunc(p int, name string, args []node) *funcNode {
	return &funcNode{
		ReturnType: ReturnNum,
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

func (f *funcNode) ReturnBool(v Vars, fs Funcs) (bool, error) {
	return false, errWrongRType
}
func (f *funcNode) ReturnStr(v Vars, fs Funcs) (string, error) {
	return "", errWrongRType
}
func (f *funcNode) ReturnNum(v Vars, fs Funcs) (float64, error) {
	fnc, ok := fs[f.name]
	if !ok {
		return 0, fmt.Errorf("undefined function %q", f.name)
	}

	args := make([]float64, len(f.args))
	for i, a := range f.args {
		r, err := a.ReturnNum(v, fs)
		if err != nil {
			return 0, err
		}
		args[i] = r
	}
	r, err := fnc.Call(args...)
	return r, err
}
