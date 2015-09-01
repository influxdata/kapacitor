package dsl

import (
	"fmt"
	"reflect"
	"runtime/debug"
	"strconv"
	"time"
	"unicode"
	"unicode/utf8"
)

type unboundFunc func(obj interface{}) (interface{}, error)

type node interface {
	Type() nodeType
	String() string
	Position() int                        // byte position of start of node in full original input string
	Check() error                         // performs type checking for itself and sub-nodes
	RType() returnType                    // the return type of the node
	Return(s *Scope) (interface{}, error) // the return value of the node
}

// nodeType identifies the type of a parse tree node.
type nodeType int

func (t nodeType) Type() nodeType {
	return t
}

type pos int

func (p pos) Position() int {
	return int(p)
}

const (
	nodeBinary nodeType = iota // Binary operator: math, logical, compare
	nodeUnary                  // Unary operator: !, -
	nodeNumber                 // A numerical constant.
	nodeDur                    // A time duration
	nodeString                 //A string literal
	nodeIdent                  // An identifier
	nodeFunc                   //A function call
	nodeList                   //A node that contains N children nodes
)

// returnType identifies the return type of a node.
type returnType int

const (
	returnVoid     returnType = iota // no return type
	returnVar                        // name of a variable
	returnFunc                       // function
	returnObject                     // object
	returnNumber                     // number
	returnString                     // string literal
	returnDuration                   // time.Duration
)

func (r returnType) RType() returnType {
	return r
}

func (r returnType) String() string {
	switch r {
	case returnVoid:
		return "void"
	case returnVar:
		return "variable"
	case returnFunc:
		return "func"
	case returnObject:
		return "obj"
	case returnNumber:
		return "number"
	case returnString:
		return "string"
	case returnDuration:
		return "duration"
	}
	return "unknown return type"
}

// numberNode holds a number: signed or unsigned integer or float.
// The value is parsed and stored under all the types that can represent the value.
// This simulates in a small amount of code the behavior of Go's ideal constants.
type numberNode struct {
	nodeType
	pos
	returnType
	IsUint  bool    // Number has an unsigned integral value.
	IsFloat bool    // Number has a floating-point value.
	Uint64  uint64  // The unsigned integer value.
	Float64 float64 // The floating-point value.
	Text    string  // The original textual representation from the input.
}

// create a new number from a text string
func newNumber(p int, text string) (*numberNode, error) {
	n := &numberNode{
		nodeType:   nodeNumber,
		pos:        pos(p),
		returnType: returnNumber,
		Text:       text,
	}
	u, err := strconv.ParseUint(text, 10, 64)
	if err == nil {
		n.IsUint = true
		n.Uint64 = u
	} else {
		f, err := strconv.ParseFloat(text, 64)
		if err == nil {
			n.IsFloat = true
			n.Float64 = f
		}
	}
	if !n.IsUint && !n.IsFloat {
		return nil, fmt.Errorf("illegal number syntax: %q", text)
	}
	return n, nil
}

func (n *numberNode) String() string {
	return fmt.Sprintf("numberNode{%s}", n.Text)
}

func (n *numberNode) Check() error {
	return nil
}

func (n *numberNode) Return(s *Scope) (interface{}, error) {
	if n.IsUint {
		return n.Uint64, nil
	}
	return n.Float64, nil
}

// durationNode holds a number: signed or unsigned integer or float.
// The value is parsed and stored under all the types that can represent the value.
// This simulates in a small amount of code the behavior of Go's ideal constants.
type durationNode struct {
	nodeType
	pos
	returnType
	Dur  time.Duration //the duration
	Text string        // The original textual representation from the input.
}

// create a new number from a text string
func newDur(p int, text string) (*durationNode, error) {
	n := &durationNode{
		nodeType:   nodeDur,
		pos:        pos(p),
		returnType: returnDuration,
		Text:       text,
	}
	d, err := time.ParseDuration(text)
	if err != nil {
		return nil, err
	}
	n.Dur = d
	return n, nil
}

func (d *durationNode) String() string {
	return fmt.Sprintf("durationNode{%s}", d.Text)
}

func (d *durationNode) Check() error {
	return nil
}

func (d *durationNode) Return(s *Scope) (interface{}, error) {
	return d.Dur, nil
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
	switch b.Operator.typ {
	case tokenAsgn:
		lrt := b.Left.RType()
		if lrt != returnVar {
			return fmt.Errorf("left operand of assignment does not have variable return type: %v", b.Left)
		}
		rrt := b.Right.RType()
		if rrt != returnObject {
			return fmt.Errorf("right operand of assignment does not have object return type: %v", b.Right)
		}
	case tokenDot:
		//Two scenarios
		// 1. the dot separates an object and a function call
		// 2. the dot separates two identifiers

		//Right operator must always be a returnVar or returnObject
		rrt := b.Right.RType()
		if rrt != returnVar && rrt != returnObject {
			return fmt.Errorf("right operand of '.' must be of object or variable return type: %v", b.Right)
		}

		lrt := b.Left.RType()
		if lrt != returnFunc && lrt != returnVar {
			return fmt.Errorf("left operand of '.' must be of function or variable return type: %v", b.Right)
		}

	default:
		return fmt.Errorf("check: unknown operator %s", b.Operator.val)
	}
	for _, a := range []node{b.Left, b.Right} {
		err := a.Check()
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *binaryNode) RType() returnType {
	switch b.Operator.typ {
	case tokenAsgn:
		return returnVoid
	case tokenDot:
		return returnObject
	}
	return returnVoid
}

func (b *binaryNode) Return(s *Scope) (interface{}, error) {
	switch b.Operator.typ {
	case tokenAsgn:
		rl, err := b.Left.Return(s)
		if err != nil {
			return nil, err
		}
		rr, err := b.Right.Return(s)
		if err != nil {
			return nil, err
		}
		name := rl.(string)
		s.Set(name, rr)
		return nil, nil
	case tokenDot:
		lrt := b.Left.RType()
		rrt := b.Right.RType()

		right, err := b.Right.Return(s)
		if err != nil {
			return nil, err
		}
		var obj interface{}

		if rrt == returnVar {
			obj = s.Get(right.(string))
		} else if rrt == returnObject {
			obj = right
		} else {
			return nil, fmt.Errorf("invalide arguments to binary operator '.'")
		}

		if lrt == returnFunc {
			// object and function call
			f, err := b.Left.Return(s)
			if err != nil {
				return nil, err
			}

			if fnc, ok := f.(unboundFunc); !ok {
				return nil, fmt.Errorf("expected a unboundFunc but got %q", f)
			} else {
				ret, err := fnc(obj)
				return ret, err
			}
		} else if lrt == returnVar {
			// two identifiers

			ident, err := b.Left.Return(s)
			if err != nil {
				return nil, err
			}
			name, ok := ident.(string)
			if !ok {
				return nil, fmt.Errorf("identifier %q is not a string", ident)
			}
			name = capilatizeFirst(name)
			//Lookup field by name of obj
			v := reflect.ValueOf(obj)
			if !v.IsValid() {
				return nil, fmt.Errorf("object is not valid, cannot get field")
			}
			v = reflect.Indirect(v)
			if v.Kind() == reflect.Struct {
				field := v.FieldByName(name)
				if field.IsValid() {
					return field.Interface(), nil
				}
			}
		}
		return nil, fmt.Errorf("return: unknown operator %s", b.Operator.val)
	}
	return nil, fmt.Errorf("return: unknown operator %s", b.Operator.val)
}

//Holds the textual representation of an identifier
type identNode struct {
	nodeType
	pos
	returnType
	Ident string // The identifier
}

func newIdent(p int, ident string) *identNode {
	return &identNode{
		nodeType:   nodeIdent,
		pos:        pos(p),
		returnType: returnVar,
		Ident:      ident,
	}
}

func (i *identNode) String() string {
	return fmt.Sprintf("identNode{%s}", i.Ident)
}

func (i *identNode) Check() error {
	return nil
}

func (i *identNode) Return(s *Scope) (interface{}, error) {
	return i.Ident, nil
}

//Holds the textual representation of a string literal
type stringNode struct {
	nodeType
	pos
	returnType
	Literal string // The string literal
	Text    string //The original text
}

func newString(p int, txt string) *stringNode {
	return &stringNode{
		nodeType:   nodeString,
		pos:        pos(p),
		returnType: returnString,
		Literal:    txt[1 : len(txt)-1], //trim off quotes
		Text:       txt,
	}
}

func (s *stringNode) String() string {
	return fmt.Sprintf("stringNode{%s}", s.Literal)
}

func (s *stringNode) Check() error {
	return nil
}

func (s *stringNode) Return(sc *Scope) (interface{}, error) {
	return s.Literal, nil
}

//Holds the a function call with its args
type funcNode struct {
	nodeType
	pos
	returnType
	Func string // The identifier
	Args []node
}

func newFunc(p int, ident string, args []node) *funcNode {
	return &funcNode{
		nodeType:   nodeFunc,
		pos:        pos(p),
		returnType: returnFunc,
		Func:       ident,
		Args:       args,
	}
}

func (f *funcNode) String() string {
	return fmt.Sprintf("funcNode{%s}", f.Func)
}

func (f *funcNode) Check() error {
	for _, arg := range f.Args {
		err := arg.Check()
		if err != nil {
			return err
		}
	}
	return nil
}

type panicError struct {
	err   error
	trace string
}

func (p panicError) Error() string {
	return fmt.Sprintf("%s:\n%s", p.err.Error(), p.trace)
}

func (f *funcNode) Return(s *Scope) (interface{}, error) {
	//Define recover method
	rec := func(errp *error) {
		e := recover()
		if e != nil {
			*errp = panicError{
				err:   e.(error),
				trace: string(debug.Stack()),
			}
		}
	}
	//Return function that will call the defined func on obj
	fnc := func(obj interface{}) (_ interface{}, err error) {
		//Setup recover method if there is a panic during reflection
		defer rec(&err)
		name := capilatizeFirst(f.Func)
		v := reflect.ValueOf(obj)
		if !v.IsValid() {
			return nil, fmt.Errorf("object is not valid")
		}
		// Check for method
		method := v.MethodByName(name)
		if method.IsValid() {
			args := make([]reflect.Value, len(f.Args))
			for i, a := range f.Args {
				r, err := a.Return(s)
				if err != nil {
					return nil, err
				}
				args[i] = reflect.ValueOf(r)
			}
			ret := method.Call(args)
			if l := len(ret); l == 1 {
				return ret[0].Interface(), nil
			} else if l == 2 {
				if err, ok := ret[1].Interface().(error); !ok {
					return nil, fmt.Errorf("second return value form function must be an 'error'")
				} else {
					return ret[0].Interface(), err
				}
			} else {
				return nil, fmt.Errorf("functions must return a single value or (interface{}, error)")
			}
		}
		// Check for settable field
		v = reflect.Indirect(v)
		if len(f.Args) == 1 && v.Kind() == reflect.Struct {
			field := v.FieldByName(name)
			if field.IsValid() && field.CanSet() {
				r, err := f.Args[0].Return(s)
				if err != nil {
					return nil, err
				}
				fieldV := reflect.ValueOf(r)
				field.Set(fieldV)
				return obj, nil
			}
		}

		return nil, fmt.Errorf("No method or field %q on %v of type %q", name, obj, reflect.TypeOf(obj))
	}
	return unboundFunc(fnc), nil
}

//Holds the a function call with its args
type listNode struct {
	nodeType
	pos
	returnType
	Nodes []node
}

func newList(p int) *listNode {
	return &listNode{
		nodeType:   nodeList,
		pos:        pos(p),
		returnType: returnObject,
	}
}

func (l *listNode) Add(n node) {
	l.Nodes = append(l.Nodes, n)
}

func (l *listNode) String() string {
	return fmt.Sprintf("listNode{%v}", l.Nodes)
}

func (l *listNode) Check() error {
	for _, n := range l.Nodes {
		err := n.Check()
		if err != nil {
			return err
		}
	}
	return nil
}

func (l *listNode) Return(s *Scope) (interface{}, error) {
	returns := make([]interface{}, len(l.Nodes))
	for i, n := range l.Nodes {
		r, err := n.Return(s)
		if err != nil {
			return nil, err
		}
		returns[i] = r
	}
	return returns, nil
}

// Capilatizes the first rune in the string
func capilatizeFirst(s string) string {
	r, n := utf8.DecodeRuneInString(s)
	s = string(unicode.ToUpper(r)) + s[n:]
	return s
}
