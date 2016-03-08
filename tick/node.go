package tick

import (
	"bytes"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/influxdata/influxdb/influxql"
)

type unboundFunc func(obj interface{}) (interface{}, error)

type Node interface {
	String() string
	Position() int // byte position of start of node in full original input string
}

type pos int

func (p pos) Position() int {
	return int(p)
}

// numberNode holds a number: signed or unsigned integer or float.
// The value is parsed and stored under all the types that can represent the value.
// This simulates in a small amount of code the behavior of Go's ideal constants.
type NumberNode struct {
	pos
	IsInt   bool    // Number has an integral value.
	IsFloat bool    // Number has a floating-point value.
	Int64   int64   // The integer value.
	Float64 float64 // The floating-point value.
}

// create a new number from a text string
func newNumber(p int, text string) (*NumberNode, error) {
	n := &NumberNode{
		pos: pos(p),
	}
	i, err := strconv.ParseInt(text, 10, 64)
	if err == nil {
		n.IsInt = true
		n.Int64 = i
		if n.Int64 < 0 {
			panic("parser should not allow for negative number nodes")
		}
	} else {
		f, err := strconv.ParseFloat(text, 64)
		if err == nil {
			n.IsFloat = true
			n.Float64 = f
			if n.Float64 < 0 {
				panic("parser should not allow for negative number nodes")
			}
		}
	}
	if !n.IsInt && !n.IsFloat {
		return nil, fmt.Errorf("illegal number syntax: %q", text)
	}
	return n, nil
}

func (n *NumberNode) String() string {
	if n.IsInt {
		return fmt.Sprintf("NumberNode@%d{%di}", n.pos, n.Int64)
	}
	return fmt.Sprintf("NumberNode@%d{%f}", n.pos, n.Float64)
}

// durationNode holds a number: signed or unsigned integer or float.
// The value is parsed and stored under all the types that can represent the value.
// This simulates in a small amount of code the behavior of Go's ideal constants.
type DurationNode struct {
	pos
	Dur time.Duration //the duration
}

// create a new number from a text string
func newDur(p int, text string) (*DurationNode, error) {
	n := &DurationNode{
		pos: pos(p),
	}
	d, err := influxql.ParseDuration(text)
	if err != nil {
		return nil, err
	}
	n.Dur = d
	return n, nil
}

func (d *DurationNode) String() string {
	return fmt.Sprintf("DurationNode@%d{%v}", d.pos, d.Dur)
}

// boolNode holds one argument and an operator.
type BoolNode struct {
	pos
	Bool bool
}

func newBool(p int, text string) (*BoolNode, error) {
	b, err := strconv.ParseBool(text)
	if err != nil {
		return nil, err
	}
	return &BoolNode{
		pos:  pos(p),
		Bool: b,
	}, nil
}

func (b *BoolNode) String() string {
	return fmt.Sprintf("BoolNode@%d{%v}", b.pos, b.Bool)
}

// unaryNode holds one argument and an operator.
type UnaryNode struct {
	pos
	Node     Node
	Operator tokenType
}

func newUnary(operator token, n Node) *UnaryNode {
	return &UnaryNode{
		pos:      pos(operator.pos),
		Node:     n,
		Operator: operator.typ,
	}
}

func (u *UnaryNode) String() string {
	return fmt.Sprintf("UnaryNode@%d{%s %s}", u.pos, u.Operator, u.Node)
}

// binaryNode holds two arguments and an operator.
type BinaryNode struct {
	pos
	Left     Node
	Right    Node
	Operator tokenType
}

func newBinary(operator token, left, right Node) *BinaryNode {
	return &BinaryNode{
		pos:      pos(operator.pos),
		Left:     left,
		Right:    right,
		Operator: operator.typ,
	}
}

func (b *BinaryNode) String() string {
	return fmt.Sprintf("BinaryNode@%d{%v %v %v}", b.pos, b.Left, b.Operator, b.Right)
}

//Holds the textual representation of an identifier
type IdentifierNode struct {
	pos
	Ident string // The identifier
}

func newIdent(p int, ident string) *IdentifierNode {
	return &IdentifierNode{
		pos:   pos(p),
		Ident: ident,
	}
}

func (i *IdentifierNode) String() string {
	return fmt.Sprintf("IdentifierNode@%d{%s}", i.pos, i.Ident)
}

//Holds the textual representation of an identifier
type ReferenceNode struct {
	pos
	Reference string // The field reference
}

func newReference(p int, txt string) *ReferenceNode {
	// Remove leading and trailing quotes
	literal := txt[1 : len(txt)-1]
	// Unescape quotes
	var buf bytes.Buffer
	buf.Grow(len(literal))
	last := 0
	for i := 0; i < len(literal)-1; i++ {
		if literal[i] == '\\' && literal[i+1] == '"' {
			buf.Write([]byte(literal[last:i]))
			i++
			last = i
		}
	}
	buf.Write([]byte(literal[last:]))
	literal = buf.String()

	return &ReferenceNode{
		pos:       pos(p),
		Reference: literal,
	}
}

func (r *ReferenceNode) String() string {
	return fmt.Sprintf("ReferenceNode@%d{%s}", r.pos, r.Reference)
}

//Holds the textual representation of a string literal
type StringNode struct {
	pos
	Literal string // The string literal
}

func newString(p int, txt string) *StringNode {

	// Remove leading and trailing quotes
	var literal string
	if len(txt) >= 6 && txt[0:3] == "'''" {
		literal = txt[3 : len(txt)-3]
	} else {
		literal = txt[1 : len(txt)-1]
		quote := txt[0]
		// Unescape quotes
		var buf bytes.Buffer
		buf.Grow(len(literal))
		last := 0
		for i := 0; i < len(literal)-1; i++ {
			if literal[i] == '\\' && literal[i+1] == quote {
				buf.Write([]byte(literal[last:i]))
				i++
				last = i
			}
		}
		buf.Write([]byte(literal[last:]))
		literal = buf.String()
	}

	return &StringNode{
		pos:     pos(p),
		Literal: literal,
	}
}

func (s *StringNode) String() string {
	return fmt.Sprintf("StringNode@%d{%s}", s.pos, s.Literal)
}

//Holds the textual representation of a regex literal
type RegexNode struct {
	pos
	Regex *regexp.Regexp
}

func newRegex(p int, txt string) (*RegexNode, error) {

	// Remove leading and trailing quotes
	literal := txt[1 : len(txt)-1]
	// Unescape slashes '/'
	var buf bytes.Buffer
	buf.Grow(len(literal))
	last := 0
	for i := 0; i < len(literal)-1; i++ {
		if literal[i] == '\\' && literal[i+1] == '/' {
			buf.Write([]byte(literal[last:i]))
			i++
			last = i
		}
	}
	buf.Write([]byte(literal[last:]))
	literal = buf.String()

	r, err := regexp.Compile(literal)
	if err != nil {
		return nil, err
	}

	return &RegexNode{
		pos:   pos(p),
		Regex: r,
	}, nil
}

func (s *RegexNode) String() string {
	return fmt.Sprintf("RegexNode@%d{%v}", s.pos, s.Regex)
}

// Represents a standalone '*' token.
type StarNode struct {
	pos
}

func newStar(p int) *StarNode {
	return &StarNode{
		pos: pos(p),
	}
}

func (s *StarNode) String() string {
	return fmt.Sprintf("StarNode@%d{*}", s.pos)
}

//Holds the a function call with its args
type FunctionNode struct {
	pos
	Func string // The identifier
	Args []Node
}

func newFunc(p int, ident string, args []Node) *FunctionNode {
	return &FunctionNode{
		pos:  pos(p),
		Func: ident,
		Args: args,
	}
}

func (f *FunctionNode) String() string {
	return fmt.Sprintf("FunctionNode@%d{%s %v}", f.pos, f.Func, f.Args)
}

// Represents the begining of a lambda expression
type LambdaNode struct {
	pos
	Node Node
}

func newLambda(p int, node Node) *LambdaNode {
	return &LambdaNode{
		pos:  pos(p),
		Node: node,
	}
}

func (l *LambdaNode) String() string {
	return fmt.Sprintf("LambdaNode@%d{%v}", l.pos, l.Node)
}

//Holds a function call with its args
type ListNode struct {
	pos
	Nodes []Node
}

func newList(p int) *ListNode {
	return &ListNode{
		pos: pos(p),
	}
}

func (l *ListNode) Add(n Node) {
	l.Nodes = append(l.Nodes, n)
}

func (l *ListNode) String() string {
	return fmt.Sprintf("ListNode@%d{%v}", l.pos, l.Nodes)
}
