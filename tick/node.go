package tick

import (
	"bytes"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/influxdata/influxdb/influxql"
)

type unboundFunc func(obj interface{}) (interface{}, error)

type Position interface {
	Position() int // byte position of start of node in full original input string
	Line() int
	Char() int
}

type Node interface {
	Position
	String() string
	Format(buf *bytes.Buffer, indent string, onNewLine bool)
}

func writeIndent(buf *bytes.Buffer, indent string, onNewLine bool) {
	if onNewLine {
		buf.WriteString(indent)
	}
}

type position struct {
	pos  int
	line int
	char int
}

func (p position) Position() int {
	return p.pos
}
func (p position) Line() int {
	return p.line
}
func (p position) Char() int {
	return p.char
}
func (p position) String() string {
	return fmt.Sprintf("%dl%dc%d", p.pos, p.line, p.char)
}

// numberNode holds a number: signed or unsigned integer or float.
// The value is parsed and stored under all the types that can represent the value.
// This simulates in a small amount of code the behavior of Go's ideal constants.
type NumberNode struct {
	position
	IsInt   bool    // Number has an integral value.
	IsFloat bool    // Number has a floating-point value.
	Int64   int64   // The integer value.
	Float64 float64 // The floating-point value.
	Comment Node
}

// create a new number from a text string
func newNumber(p position, text string, c Node) (*NumberNode, error) {
	n := &NumberNode{
		position: p,
		Comment:  c,
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
		return fmt.Sprintf("NumberNode@%v{%di}%v", n.position, n.Int64, n.Comment)
	}
	return fmt.Sprintf("NumberNode@%v{%f}%v", n.position, n.Float64, n.Comment)
}

func (n *NumberNode) Format(buf *bytes.Buffer, indent string, onNewLine bool) {
	if n.Comment != nil {
		n.Comment.Format(buf, indent, onNewLine)
		onNewLine = true
	}
	writeIndent(buf, indent, onNewLine)
	if n.IsInt {
		buf.WriteString(strconv.FormatInt(n.Int64, 10))
	} else {
		s := strconv.FormatFloat(n.Float64, 'f', -1, 64)
		if strings.IndexRune(s, '.') == -1 {
			s += ".0"
		}
		buf.WriteString(s)
	}
}

// durationNode holds a number: signed or unsigned integer or float.
// The value is parsed and stored under all the types that can represent the value.
// This simulates in a small amount of code the behavior of Go's ideal constants.
type DurationNode struct {
	position
	Dur     time.Duration //the duration
	Comment Node
}

// create a new number from a text string
func newDur(p position, text string, c Node) (*DurationNode, error) {
	n := &DurationNode{
		position: p,
		Comment:  c,
	}
	d, err := influxql.ParseDuration(text)
	if err != nil {
		return nil, err
	}
	n.Dur = d
	return n, nil
}

func (n *DurationNode) String() string {
	return fmt.Sprintf("DurationNode@%v{%v}%v", n.position, n.Dur, n.Comment)
}

func (n *DurationNode) Format(buf *bytes.Buffer, indent string, onNewLine bool) {
	if n.Comment != nil {
		n.Comment.Format(buf, indent, onNewLine)
		onNewLine = true
	}
	writeIndent(buf, indent, onNewLine)
	buf.WriteString(influxql.FormatDuration(n.Dur))
}

// boolNode holds one argument and an operator.
type BoolNode struct {
	position
	Bool    bool
	Comment Node
}

func newBool(p position, text string, c Node) (*BoolNode, error) {
	b, err := strconv.ParseBool(text)
	if err != nil {
		return nil, err
	}
	return &BoolNode{
		position: p,
		Bool:     b,
		Comment:  c,
	}, nil
}

func (n *BoolNode) String() string {
	return fmt.Sprintf("BoolNode@%v{%v}%v", n.position, n.Bool, n.Comment)
}
func (n *BoolNode) Format(buf *bytes.Buffer, indent string, onNewLine bool) {
	if n.Comment != nil {
		n.Comment.Format(buf, indent, onNewLine)
		onNewLine = true
	}
	writeIndent(buf, indent, onNewLine)
	if n.Bool {
		buf.WriteString(KW_True)
	} else {
		buf.WriteString(KW_False)
	}
}

// unaryNode holds one argument and an operator.
type UnaryNode struct {
	position
	Node     Node
	Operator tokenType
	Comment  Node
}

func newUnary(p position, op tokenType, n Node, c Node) *UnaryNode {
	return &UnaryNode{
		position: p,
		Node:     n,
		Operator: op,
		Comment:  c,
	}
}

func (n *UnaryNode) String() string {
	return fmt.Sprintf("UnaryNode@%v{%s %s}%v", n.position, n.Operator, n.Node, n.Comment)
}

func (n *UnaryNode) Format(buf *bytes.Buffer, indent string, onNewLine bool) {
	if n.Comment != nil {
		n.Comment.Format(buf, indent, onNewLine)
		onNewLine = true
	}
	writeIndent(buf, indent, onNewLine)
	buf.WriteString(n.Operator.String())
	n.Node.Format(buf, indent, false)
}

// binaryNode holds two arguments and an operator.
type BinaryNode struct {
	position
	Left      Node
	Right     Node
	Operator  tokenType
	Comment   Node
	Parens    bool
	MultiLine bool
}

func newBinary(p position, op tokenType, left, right Node, multiLine bool, comment Node) *BinaryNode {
	return &BinaryNode{
		position:  p,
		Left:      left,
		Right:     right,
		Operator:  op,
		MultiLine: multiLine,
		Comment:   comment,
	}
}

func (n *BinaryNode) String() string {
	return fmt.Sprintf("BinaryNode@%v{p:%v m:%v %v %v %v}%v", n.position, n.Parens, n.MultiLine, n.Left, n.Operator, n.Right, n.Comment)
}

func (n *BinaryNode) Format(buf *bytes.Buffer, indent string, onNewLine bool) {
	if n.Comment != nil {
		n.Comment.Format(buf, indent, onNewLine)
		onNewLine = true
	}
	writeIndent(buf, indent, onNewLine)
	if n.Parens {
		buf.WriteByte('(')
		indent += indentStep
	}
	n.Left.Format(buf, indent, false)
	buf.WriteByte(' ')
	buf.WriteString(n.Operator.String())
	if n.MultiLine {
		buf.WriteByte('\n')
	} else {
		buf.WriteByte(' ')
	}
	n.Right.Format(buf, indent, n.MultiLine)
	if n.Parens {
		buf.WriteByte(')')
	}
}

type DeclarationNode struct {
	position
	Left    *IdentifierNode
	Right   Node
	Comment Node
}

func newDecl(p position, left *IdentifierNode, right Node, comment Node) *DeclarationNode {
	return &DeclarationNode{
		position: p,
		Left:     left,
		Right:    right,
		Comment:  comment,
	}
}

func (n *DeclarationNode) String() string {
	return fmt.Sprintf("DeclarationNode@%v{%v %v}%v", n.position, n.Left, n.Right, n.Comment)
}

func (n *DeclarationNode) Format(buf *bytes.Buffer, indent string, onNewLine bool) {
	if n.Comment != nil {
		n.Comment.Format(buf, indent, onNewLine)
	}
	buf.WriteString(KW_Var)
	buf.WriteByte(' ')
	n.Left.Format(buf, indent, false)
	buf.WriteByte(' ')
	buf.WriteString(TokenAsgn.String())
	buf.WriteByte(' ')
	n.Right.Format(buf, indent, false)
}

type ChainNode struct {
	position
	Left     Node
	Right    Node
	Operator tokenType
	Comment  Node
}

func newChain(p position, op tokenType, left, right Node, comment Node) *ChainNode {
	return &ChainNode{
		position: p,
		Left:     left,
		Right:    right,
		Operator: op,
		Comment:  comment,
	}
}

func (n *ChainNode) String() string {
	return fmt.Sprintf("ChainNode@%v{%v %v %v}%v", n.position, n.Left, n.Operator, n.Right, n.Comment)
}

func (n *ChainNode) Format(buf *bytes.Buffer, indent string, onNewLine bool) {
	n.Left.Format(buf, indent, onNewLine)
	buf.WriteByte('\n')
	indent = indent + indentStep
	if n.Operator == TokenDot {
		indent = indent + indentStep
	}
	if n.Comment != nil {
		n.Comment.Format(buf, indent, true)
	}
	buf.WriteString(indent)
	buf.WriteString(n.Operator.String())
	n.Right.Format(buf, indent, false)
}

//Holds the textual representation of an identifier
type IdentifierNode struct {
	position
	Ident   string // The identifier
	Comment Node
}

func newIdent(p position, ident string, c Node) *IdentifierNode {
	return &IdentifierNode{
		position: p,
		Ident:    ident,
		Comment:  c,
	}
}

func (n *IdentifierNode) String() string {
	return fmt.Sprintf("IdentifierNode@%v{%s}%v", n.position, n.Ident, n.Comment)
}

func (n *IdentifierNode) Format(buf *bytes.Buffer, indent string, onNewLine bool) {
	if n.Comment != nil {
		n.Comment.Format(buf, indent, onNewLine)
		onNewLine = true
	}
	writeIndent(buf, indent, onNewLine)
	buf.WriteString(n.Ident)
}

//Holds the textual representation of an identifier
type ReferenceNode struct {
	position
	Reference string // The field reference
	Comment   Node
}

func newReference(p position, txt string, c Node) *ReferenceNode {
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
		position:  p,
		Reference: literal,
		Comment:   c,
	}
}

func (n *ReferenceNode) String() string {
	return fmt.Sprintf("ReferenceNode@%v{%s}%v", n.position, n.Reference, n.Comment)
}

func (n *ReferenceNode) Format(buf *bytes.Buffer, indent string, onNewLine bool) {
	if n.Comment != nil {
		n.Comment.Format(buf, indent, onNewLine)
		onNewLine = true
	}
	writeIndent(buf, indent, onNewLine)
	buf.WriteByte('"')
	for _, c := range n.Reference {
		if c == '"' {
			buf.WriteByte('\\')
		}
		buf.WriteRune(c)
	}
	buf.WriteByte('"')
}

//Holds the textual representation of a string literal
type StringNode struct {
	position
	Literal      string // The string literal
	TripleQuotes bool
	Comment      Node
}

func newString(p position, txt string, c Node) *StringNode {

	tripleQuotes := false
	// Remove leading and trailing quotes
	var literal string
	if len(txt) >= 6 && txt[0:3] == "'''" {
		literal = txt[3 : len(txt)-3]
		tripleQuotes = true
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
		position:     p,
		Literal:      literal,
		TripleQuotes: tripleQuotes,
		Comment:      c,
	}
}

func (n *StringNode) String() string {
	return fmt.Sprintf("StringNode@%v{%s}%v", n.position, n.Literal, n.Comment)
}

func (n *StringNode) Format(buf *bytes.Buffer, indent string, onNewLine bool) {
	if n.Comment != nil {
		n.Comment.Format(buf, indent, onNewLine)
		onNewLine = true
	}
	writeIndent(buf, indent, onNewLine)
	if n.TripleQuotes {
		buf.WriteString("'''")
	} else {
		buf.WriteByte('\'')
	}
	if n.TripleQuotes {
		buf.WriteString(n.Literal)
	} else {
		for _, c := range n.Literal {
			if c == '\'' {
				buf.WriteByte('\\')
			}
			buf.WriteRune(c)
		}
	}
	if n.TripleQuotes {
		buf.WriteString("'''")
	} else {
		buf.WriteByte('\'')
	}
}

//Holds the textual representation of a regex literal
type RegexNode struct {
	position
	Regex   *regexp.Regexp
	Comment Node
}

func newRegex(p position, txt string, c Node) (*RegexNode, error) {

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
		position: p,
		Regex:    r,
		Comment:  c,
	}, nil
}

func (n *RegexNode) String() string {
	return fmt.Sprintf("RegexNode@%v{%v}%v", n.position, n.Regex, n.Comment)
}

func (n *RegexNode) Format(buf *bytes.Buffer, indent string, onNewLine bool) {
	if n.Comment != nil {
		n.Comment.Format(buf, indent, onNewLine)
		onNewLine = true
	}
	writeIndent(buf, indent, onNewLine)
	buf.WriteByte('/')
	buf.WriteString(n.Regex.String())
	buf.WriteByte('/')
}

// Represents a standalone '*' token.
type StarNode struct {
	position
	Comment Node
}

func newStar(p position, c Node) *StarNode {
	return &StarNode{
		position: p,
		Comment:  c,
	}
}

func (n *StarNode) String() string {
	return fmt.Sprintf("StarNode@%v{*}%v", n.position, n.Comment)
}

func (n *StarNode) Format(buf *bytes.Buffer, indent string, onNewLine bool) {
	if n.Comment != nil {
		n.Comment.Format(buf, indent, onNewLine)
		onNewLine = true
	}
	writeIndent(buf, indent, onNewLine)
	buf.WriteByte('*')
}

type funcType int

const (
	globalFunc funcType = iota
	chainFunc
	propertyFunc
	dynamicFunc
)

func (ft funcType) String() string {
	switch ft {
	case globalFunc:
		return "global"
	case chainFunc:
		return "chain"
	case propertyFunc:
		return "property"
	case dynamicFunc:
		return "dynamic"
	default:
		return "unknown"
	}
}

//Holds the a function call with its args
type FunctionNode struct {
	position
	Type      funcType
	Func      string // The identifier
	Args      []Node
	Comment   Node
	MultiLine bool
}

func newFunc(p position, ft funcType, ident string, args []Node, multi bool, c Node) *FunctionNode {
	return &FunctionNode{
		position:  p,
		Type:      ft,
		Func:      ident,
		Args:      args,
		Comment:   c,
		MultiLine: multi,
	}
}

func (n *FunctionNode) String() string {
	return fmt.Sprintf("FunctionNode@%v{%v %s %v}%v", n.position, n.Type, n.Func, n.Args, n.Comment)
}

func (n *FunctionNode) Format(buf *bytes.Buffer, indent string, onNewLine bool) {
	if n.Comment != nil {
		n.Comment.Format(buf, indent, onNewLine)
		onNewLine = true
	}
	writeIndent(buf, indent, onNewLine)
	buf.WriteString(n.Func)
	buf.WriteByte('(')
	argIndent := indent + indentStep
	for i, arg := range n.Args {
		if i != 0 {
			buf.WriteByte(',')
			if !n.MultiLine {
				buf.WriteByte(' ')
			}
		}
		if n.MultiLine {
			buf.WriteByte('\n')
		}
		arg.Format(buf, argIndent, n.MultiLine)
	}

	if n.MultiLine && len(n.Args) > 0 {
		buf.WriteByte('\n')
		buf.WriteString(indent)
	}
	buf.WriteByte(')')
}

// Represents the begining of a lambda expression
type LambdaNode struct {
	position
	Node    Node
	Comment Node
}

func newLambda(p position, node Node, c Node) *LambdaNode {
	return &LambdaNode{
		position: p,
		Node:     node,
		Comment:  c,
	}
}

func (n *LambdaNode) String() string {
	return fmt.Sprintf("LambdaNode@%v{%v}%v", n.position, n.Node, n.Comment)
}

func (n *LambdaNode) Format(buf *bytes.Buffer, indent string, onNewLine bool) {
	if n.Comment != nil {
		n.Comment.Format(buf, indent, onNewLine)
		onNewLine = true
	}
	writeIndent(buf, indent, onNewLine)
	buf.WriteString("lambda: ")
	n.Node.Format(buf, indent, false)
}

//Holds a function call with its args
type ListNode struct {
	position
	Nodes []Node
}

func newList(p position) *ListNode {
	return &ListNode{
		position: p,
	}
}

func (n *ListNode) Add(node Node) {
	n.Nodes = append(n.Nodes, node)
}

func (n *ListNode) String() string {
	return fmt.Sprintf("ListNode@%v{%v}", n.position, n.Nodes)
}

func (n *ListNode) Format(buf *bytes.Buffer, indent string, onNewLine bool) {
	for i, node := range n.Nodes {
		if i != 0 {
			buf.WriteByte('\n')
		}
		node.Format(buf, indent, true)
		buf.WriteByte('\n')
	}
}

// Hold the contents of a comment
type CommentNode struct {
	position
	Comments []string
}

func newComment(p position, comments []string) *CommentNode {
	for i := range comments {
		comments[i] = strings.TrimSpace(comments[i])
		comments[i] = strings.TrimLeftFunc(comments[i][2:], unicode.IsSpace)
	}
	return &CommentNode{
		position: p,
		Comments: comments,
	}
}

func (n *CommentNode) String() string {
	return fmt.Sprintf("CommentNode@%v{%v}", n.position, n.Comments)
}

func (n *CommentNode) Format(buf *bytes.Buffer, indent string, onNewLine bool) {
	if !onNewLine {
		buf.WriteByte('\n')
	}
	for _, comment := range n.Comments {
		buf.WriteString(indent)
		buf.WriteString("//")
		if len(comment) > 0 {
			buf.WriteByte(' ')
			buf.WriteString(comment)
		}
		buf.WriteByte('\n')
	}
}
