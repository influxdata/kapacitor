package ast

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/influxdb/influxql"
)

// Indent string for formatted TICKscripts
const indentStep = "    "

type Position interface {
	Position() int // byte position of start of node in full original input string
	Line() int
	Char() int
}

type Node interface {
	Position
	String() string
	Format(buf *bytes.Buffer, indent string, onNewLine bool)
	// Report whether to nodes are functionally equal, ignoring position and comments
	Equal(interface{}) bool

	json.Marshaler
	json.Unmarshaler
	unmarshal(JSONNode) error
}

func Format(n Node) string {
	var buf bytes.Buffer
	n.Format(&buf, "", false)
	return buf.String()
}

// Represents a node that can have a comment associated with it.
type commentedNode interface {
	SetComment(c *CommentNode)
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

const (
	octal   = 8
	decimal = 10
)

// numberNode holds a number: signed or unsigned integer or float.
// The value is parsed and stored under all the types that can represent the value.
// This simulates in a small amount of code the behavior of Go's ideal constants.
type NumberNode struct {
	position
	IsInt   bool    // Number has an integral value.
	IsFloat bool    // Number has a floating-point value.
	Int64   int64   // The integer value.
	Float64 float64 // The floating-point value.
	Base    int     // The base of an integer value.
	Comment *CommentNode
}

// create a new number from a text string
func newNumber(p position, text string, c *CommentNode) (*NumberNode, error) {
	n := &NumberNode{
		position: p,
		Comment:  c,
	}

	if text == "" {
		return nil, errors.New("invalid number literal, empty string")
	}

	if s := strings.IndexRune(text, '.'); s != -1 {
		f, err := strconv.ParseFloat(text, 64)
		if err == nil {
			n.IsFloat = true
			n.Float64 = f
			if n.Float64 < 0 {
				return nil, errors.New("parser should not allow for negative number nodes")
			}
		}
	} else {
		if text[0] == '0' && len(text) > 1 {
			// We have an octal number
			n.Base = octal
		} else {
			n.Base = decimal
		}
		i, err := strconv.ParseInt(text, n.Base, 64)
		if err == nil {
			n.IsInt = true
			n.Int64 = i
		}
		if n.Int64 < 0 {
			return nil, errors.New("parser should not allow for negative number nodes")
		}
	}

	if !n.IsInt && !n.IsFloat {
		return nil, fmt.Errorf("illegal number syntax: %q", text)
	}
	return n, nil
}

func (n *NumberNode) String() string {
	if n.IsInt {
		return fmt.Sprintf("NumberNode@%v{%di,b%d}%v", n.position, n.Int64, n.Base, n.Comment)
	}
	return fmt.Sprintf("NumberNode@%v{%f,b%d}%v", n.position, n.Float64, n.Base, n.Comment)
}

func (n *NumberNode) Format(buf *bytes.Buffer, indent string, onNewLine bool) {
	if n.Comment != nil {
		n.Comment.Format(buf, indent, onNewLine)
		onNewLine = true
	}
	writeIndent(buf, indent, onNewLine)
	if n.IsInt {
		if n.Base == octal {
			buf.WriteByte('0')
		}
		buf.WriteString(strconv.FormatInt(n.Int64, n.Base))
	} else {
		s := strconv.FormatFloat(n.Float64, 'f', -1, 64)
		if strings.IndexRune(s, '.') == -1 {
			s += ".0"
		}
		buf.WriteString(s)
	}
}
func (n *NumberNode) SetComment(c *CommentNode) {
	n.Comment = c
}

func (n *NumberNode) Equal(o interface{}) bool {
	if on, ok := o.(*NumberNode); ok {
		return n.IsInt == on.IsInt &&
			n.IsFloat == on.IsFloat &&
			n.Int64 == on.Int64 &&
			n.Float64 == on.Float64
	}
	return false
}

// MarshalJSON converts the node to JSON with an additional
// typeOf field.
func (n *NumberNode) MarshalJSON() ([]byte, error) {
	props := JSONNode{}.
		Type("number").
		Set("isint", n.IsInt).
		Set("isfloat", n.IsFloat).
		Set("int64", n.Int64).
		Set("float64", n.Float64).
		Set("base", n.Base)

	return json.Marshal(&props)
}

func (n *NumberNode) unmarshal(props JSONNode) error {
	err := props.CheckTypeOf("number")
	if err != nil {
		return err
	}

	if n.IsInt, err = props.Bool("isint"); err != nil {
		return err
	}
	if n.IsInt {
		if n.Int64, err = props.Int64("int64"); err != nil {
			return err
		}
		base, err := props.Int64("base")
		if err != nil {
			return err
		}
		if base == 0 {
			return fmt.Errorf("integer base cannot be zero")
		}
		n.Base = int(base)
	}

	if n.IsFloat, err = props.Bool("isfloat"); err != nil {
		return err
	}
	if n.IsFloat {
		n.Float64, err = props.Float64("float64")
		return err
	}

	return nil
}

// UnmarshalJSON converts JSON bytes to a Number node.
func (n *NumberNode) UnmarshalJSON(data []byte) error {
	var props JSONNode
	err := json.Unmarshal(data, &props)
	if err != nil {
		return err
	}
	return n.unmarshal(props)
}

// durationNode holds a number: signed or unsigned integer or float.
// The value is parsed and stored under all the types that can represent the value.
// This simulates in a small amount of code the behavior of Go's ideal constants.
type DurationNode struct {
	position
	Dur     time.Duration //the duration
	Literal string
	Comment *CommentNode
}

// MarshalJSON converts the node to JSON with an additional
// typeOf field.
func (n *DurationNode) MarshalJSON() ([]byte, error) {
	props := JSONNode{}.Type("duration").SetDuration("duration", n.Dur)
	return json.Marshal(&props)
}

func (n *DurationNode) unmarshal(props JSONNode) error {
	err := props.CheckTypeOf("duration")
	if err != nil {
		return err
	}

	if n.Dur, err = props.Duration("duration"); err != nil {
		return err
	}

	return nil
}

// UnmarshalJSON converts JSON bytes to a DurationNode.
func (n *DurationNode) UnmarshalJSON(data []byte) error {
	var props JSONNode
	err := json.Unmarshal(data, &props)
	if err != nil {
		return err
	}
	return n.unmarshal(props)
}

// create a new number from a text string
func newDur(p position, text string, c *CommentNode) (*DurationNode, error) {
	n := &DurationNode{
		position: p,
		Comment:  c,
		Literal:  text,
	}
	d, err := influxql.ParseDuration(text)
	if err != nil {
		return nil, err
	}
	n.Dur = d
	return n, nil
}

func (n *DurationNode) String() string {
	return fmt.Sprintf("DurationNode@%v{%v,%q}%v", n.position, n.Dur, n.Literal, n.Comment)
}

func (n *DurationNode) Format(buf *bytes.Buffer, indent string, onNewLine bool) {
	if n.Comment != nil {
		n.Comment.Format(buf, indent, onNewLine)
		onNewLine = true
	}
	writeIndent(buf, indent, onNewLine)
	if n.Literal == "" {
		n.Literal = influxql.FormatDuration(n.Dur)
	}
	buf.WriteString(n.Literal)
}
func (n *DurationNode) SetComment(c *CommentNode) {
	n.Comment = c
}

func (n *DurationNode) Equal(o interface{}) bool {
	if on, ok := o.(*DurationNode); ok {
		return n.Dur == on.Dur
	}
	return false
}

// boolNode holds one argument and an operator.
type BoolNode struct {
	position
	Bool    bool
	Comment *CommentNode
}

// MarshalJSON converts the node to JSON with an additional
// typeOf field.
func (n *BoolNode) MarshalJSON() ([]byte, error) {
	props := JSONNode{}.Type("bool").Set("bool", n.Bool)
	return json.Marshal(&props)
}

func (n *BoolNode) unmarshal(props JSONNode) error {
	err := props.CheckTypeOf("bool")
	if err != nil {
		return err
	}

	if n.Bool, err = props.Bool("bool"); err != nil {
		return err
	}
	return nil
}

// UnmarshalJSON converts JSON bytes to a BoolNode
func (n *BoolNode) UnmarshalJSON(data []byte) error {
	var props JSONNode
	err := json.Unmarshal(data, &props)
	if err != nil {
		return err
	}
	return n.unmarshal(props)
}

func newBool(p position, text string, c *CommentNode) (*BoolNode, error) {
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
func (n *BoolNode) SetComment(c *CommentNode) {
	n.Comment = c
}
func (n *BoolNode) Equal(o interface{}) bool {
	if on, ok := o.(*BoolNode); ok {
		return n.Bool == on.Bool
	}
	return false
}

// unaryNode holds one argument and an operator.
type UnaryNode struct {
	position
	Node     Node
	Operator TokenType
	Comment  *CommentNode
}

// MarshalJSON converts the node to JSON with an additional
// typeOf field.
func (n *UnaryNode) MarshalJSON() ([]byte, error) {
	props := JSONNode{}.
		Type("unary").
		SetOperator("operator", n.Operator).
		Set("node", n.Node)
	return json.Marshal(&props)
}

func (n *UnaryNode) unmarshal(props JSONNode) error {
	err := props.CheckTypeOf("unary")
	if err != nil {
		return err
	}

	if n.Operator, err = props.Operator("operator"); err != nil {
		return err
	}

	if n.Node, err = props.Node("node"); err != nil {
		return err
	}
	return nil
}

// UnmarshalJSON converts JSON bytes to a UnaryNode
func (n *UnaryNode) UnmarshalJSON(data []byte) error {
	var props JSONNode
	err := json.Unmarshal(data, &props)
	if err != nil {
		return err
	}
	return n.unmarshal(props)
}

func newUnary(p position, op TokenType, n Node, c *CommentNode) *UnaryNode {
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
func (n *UnaryNode) SetComment(c *CommentNode) {
	n.Comment = c
}

func (n *UnaryNode) Equal(o interface{}) bool {
	if on, ok := o.(*UnaryNode); ok {
		return n.Operator == on.Operator && n.Node.Equal(on.Node)
	}
	return false
}

// binaryNode holds two arguments and an operator.
type BinaryNode struct {
	position
	Left      Node
	Right     Node
	Operator  TokenType
	Comment   *CommentNode
	Parens    bool
	MultiLine bool
}

// MarshalJSON converts the node to JSON with an additional
// typeOf field.
func (n *BinaryNode) MarshalJSON() ([]byte, error) {
	props := JSONNode{}.
		Type("binary").
		SetOperator("operator", n.Operator).
		Set("left", n.Left).
		Set("right", n.Right)

	return json.Marshal(&props)
}

func (n *BinaryNode) unmarshal(props JSONNode) error {
	err := props.CheckTypeOf("binary")
	if err != nil {
		return err
	}

	if n.Operator, err = props.Operator("operator"); err != nil {
		return err
	}

	if n.Left, err = props.Node("left"); err != nil {
		return err
	}

	if n.Right, err = props.Node("right"); err != nil {
		return err
	}
	return nil
}

// UnmarshalJSON converts JSON bytes to a BinaryNode
func (n *BinaryNode) UnmarshalJSON(data []byte) error {
	var props JSONNode
	err := json.Unmarshal(data, &props)
	if err != nil {
		return err
	}
	return n.unmarshal(props)
}

func newBinary(p position, op TokenType, left, right Node, multiLine bool, c *CommentNode) *BinaryNode {
	return &BinaryNode{
		position:  p,
		Left:      left,
		Right:     right,
		Operator:  op,
		MultiLine: multiLine,
		Comment:   c,
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
func (n *BinaryNode) SetComment(c *CommentNode) {
	n.Comment = c
}

func (n *BinaryNode) Equal(o interface{}) bool {
	if on, ok := o.(*BinaryNode); ok {
		return n.Operator == on.Operator &&
			n.Left.Equal(on.Left) &&
			n.Right.Equal(on.Right)
	}
	return false
}

type DBRPNode struct {
	position
	Comment *CommentNode
	DB      *ReferenceNode
	RP      *ReferenceNode
}

func newDBRP(p position, db, rp *ReferenceNode, c *CommentNode) *DBRPNode {
	return &DBRPNode{
		position: p,
		DB:       db,
		RP:       rp,
		Comment:  c,
	}
}

// MarshalJSON converts the node to JSON with an additional
// typeOf field.
func (d *DBRPNode) MarshalJSON() ([]byte, error) {
	props := JSONNode{}.
		Type("dbrp").
		Set("db", d.DB).
		Set("rp", d.RP)
	return json.Marshal(&props)
}

func (d *DBRPNode) unmarshal(props JSONNode) error {
	err := props.CheckTypeOf("dbrp")
	if err != nil {
		return err
	}

	if d.DB, err = props.RefNode("db"); err != nil {
		return err
	}

	if d.RP, err = props.RefNode("rp"); err != nil {
		return err
	}
	return nil
}

// UnmarshalJSON converts JSON bytes to a DBRPNode
func (d *DBRPNode) UnmarshalJSON(data []byte) error {
	var props JSONNode
	err := json.Unmarshal(data, &props)
	if err != nil {
		return err
	}
	return d.unmarshal(props)
}

func (d *DBRPNode) DBRP() string {
	return "\"" + d.DB.Reference + "\"" + "." + "\"" + d.RP.Reference + "\""
}

func (d *DBRPNode) Equal(o interface{}) bool {
	if on, ok := o.(*DBRPNode); ok {
		return d.DB.Equal(on.DB) && d.RP.Equal(on.RP)
	}

	return false
}

func (s *DBRPNode) Format(buf *bytes.Buffer, indent string, onNewLine bool) {
	if s.Comment != nil {
		s.Comment.Format(buf, indent, onNewLine)
	}
	buf.WriteString(indent)
	buf.WriteString(TokenDBRP.String())
	buf.WriteByte(' ')
	buf.WriteString(s.DBRP())
}

func (n *DBRPNode) String() string {
	return fmt.Sprintf("DBRPNode@%v{%v %v}%v", n.position, n.DB, n.RP, n.Comment)
}

type DeclarationNode struct {
	position
	Left    *IdentifierNode
	Right   Node
	Comment *CommentNode
}

// MarshalJSON converts the node to JSON with an additional
// typeOf field.
func (n *DeclarationNode) MarshalJSON() ([]byte, error) {
	props := JSONNode{}.
		Type("declaration").
		Set("left", n.Left).
		Set("right", n.Right)
	return json.Marshal(&props)
}

func (n *DeclarationNode) unmarshal(props JSONNode) error {
	err := props.CheckTypeOf("declaration")
	if err != nil {
		return err
	}

	if n.Left, err = props.IDNode("left"); err != nil {
		return err
	}

	if n.Right, err = props.Node("right"); err != nil {
		return err
	}
	return nil
}

// UnmarshalJSON converts JSON bytes to a DeclarationNode
func (n *DeclarationNode) UnmarshalJSON(data []byte) error {
	var props JSONNode
	err := json.Unmarshal(data, &props)
	if err != nil {
		return err
	}
	return n.unmarshal(props)
}

func newDecl(p position, left *IdentifierNode, right Node, c *CommentNode) *DeclarationNode {
	return &DeclarationNode{
		position: p,
		Left:     left,
		Right:    right,
		Comment:  c,
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
func (n *DeclarationNode) SetComment(c *CommentNode) {
	n.Comment = c
}
func (n *DeclarationNode) Equal(o interface{}) bool {
	if on, ok := o.(*DeclarationNode); ok {
		return n.Left.Equal(on.Left) &&
			n.Right.Equal(on.Right)
	}
	return false
}

type TypeDeclarationNode struct {
	position
	Node    *IdentifierNode
	Type    *IdentifierNode
	Comment *CommentNode
}

// MarshalJSON converts the node to JSON with an additional
// typeOf field.
func (n *TypeDeclarationNode) MarshalJSON() ([]byte, error) {
	props := JSONNode{}.
		Type("typeDeclaration").
		Set("node", n.Node).
		Set("type", n.Type)
	return json.Marshal(&props)
}

func (n *TypeDeclarationNode) unmarshal(props JSONNode) error {
	err := props.CheckTypeOf("typeDeclaration")
	if err != nil {
		return err
	}

	if n.Node, err = props.IDNode("node"); err != nil {
		return err
	}

	if n.Type, err = props.IDNode("type"); err != nil {
		return err
	}
	return nil
}

// UnmarshalJSON converts JSON bytes to a TypeDeclarationNode
func (n *TypeDeclarationNode) UnmarshalJSON(data []byte) error {
	var props JSONNode
	err := json.Unmarshal(data, &props)
	if err != nil {
		return err
	}
	return n.unmarshal(props)
}

func newTypeDecl(p position, node, typeIdent *IdentifierNode, c *CommentNode) *TypeDeclarationNode {
	return &TypeDeclarationNode{
		position: p,
		Node:     node,
		Type:     typeIdent,
		Comment:  c,
	}
}

func (n *TypeDeclarationNode) String() string {
	return fmt.Sprintf("TypeDeclarationNode@%v{%v %v}%v", n.position, n.Node, n.Type, n.Comment)
}

func (n *TypeDeclarationNode) Format(buf *bytes.Buffer, indent string, onNewLine bool) {
	if n.Comment != nil {
		n.Comment.Format(buf, indent, onNewLine)
	}
	buf.WriteString(KW_Var)
	buf.WriteByte(' ')
	n.Node.Format(buf, indent, false)
	buf.WriteByte(' ')
	n.Type.Format(buf, indent, false)
}

func (n *TypeDeclarationNode) SetComment(c *CommentNode) {
	n.Comment = c
}
func (n *TypeDeclarationNode) Equal(o interface{}) bool {
	if on, ok := o.(*TypeDeclarationNode); ok {
		return n.Node.Equal(on.Node) &&
			n.Type.Equal(on.Type)
	}
	return false
}

type ChainNode struct {
	position
	Left     Node
	Right    Node
	Operator TokenType
	Comment  *CommentNode
}

// MarshalJSON converts the node to JSON with an additional
// typeOf field.
func (n *ChainNode) MarshalJSON() ([]byte, error) {
	props := JSONNode{}.
		Type(n.TypeOf()).
		SetOperator("operator", n.Operator).
		Set("left", n.Left).
		Set("right", n.Right)

	return json.Marshal(&props)
}

func (n *ChainNode) unmarshal(props JSONNode) error {
	err := props.CheckTypeOf(n.TypeOf())
	if err != nil {
		return err
	}

	if n.Operator, err = props.Operator("operator"); err != nil {
		return err
	}

	if n.Left, err = props.Node("left"); err != nil {
		return err
	}

	if n.Right, err = props.Node("right"); err != nil {
		return err
	}

	return nil
}

// UnmarshalJSON converts JSON bytes to a ChainNode
func (n *ChainNode) UnmarshalJSON(data []byte) error {
	var props JSONNode
	err := json.Unmarshal(data, &props)
	if err != nil {
		return err
	}
	return n.unmarshal(props)
}

// TypeOf returns the unique name for this type of node
func (n *ChainNode) TypeOf() string {
	return "chain"
}

func newChain(p position, op TokenType, left, right Node, c *CommentNode) *ChainNode {
	return &ChainNode{
		position: p,
		Left:     left,
		Right:    right,
		Operator: op,
		Comment:  c,
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
func (n *ChainNode) SetComment(c *CommentNode) {
	n.Comment = c
}
func (n *ChainNode) Equal(o interface{}) bool {
	if on, ok := o.(*ChainNode); ok {
		return n.Operator == on.Operator &&
			n.Left.Equal(on.Left) &&
			n.Right.Equal(on.Right)
	}
	return false
}

//Holds the textual representation of an identifier
type IdentifierNode struct {
	position
	Ident   string // The identifier
	Comment *CommentNode
}

// MarshalJSON converts the node to JSON with an additional
// typeOf field.
func (n *IdentifierNode) MarshalJSON() ([]byte, error) {
	props := JSONNode{}.
		Type("identifier").
		Set("ident", n.Ident)

	return json.Marshal(&props)
}

func (n *IdentifierNode) unmarshal(props JSONNode) error {
	err := props.CheckTypeOf("identifier")
	if err != nil {
		return err
	}

	if n.Ident, err = props.String("ident"); err != nil {
		return err
	}
	return nil
}

// UnmarshalJSON converts JSON bytes to a IdentifierNode
func (n *IdentifierNode) UnmarshalJSON(data []byte) error {
	var props JSONNode
	err := json.Unmarshal(data, &props)
	if err != nil {
		return err
	}
	return n.unmarshal(props)
}

func newIdent(p position, ident string, c *CommentNode) *IdentifierNode {
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
func (n *IdentifierNode) SetComment(c *CommentNode) {
	n.Comment = c
}
func (n *IdentifierNode) Equal(o interface{}) bool {
	if on, ok := o.(*IdentifierNode); ok {
		return n.Ident == on.Ident
	}
	return false
}

//Holds the textual representation of an identifier
type ReferenceNode struct {
	position
	Reference string // The field reference
	Comment   *CommentNode
}

// MarshalJSON converts the node to JSON with an additional
// typeOf field.
func (n *ReferenceNode) MarshalJSON() ([]byte, error) {
	props := JSONNode{}.
		Type("reference").
		Set("reference", n.Reference)

	return json.Marshal(&props)
}

func (n *ReferenceNode) unmarshal(props JSONNode) error {
	err := props.CheckTypeOf("reference")
	if err != nil {
		return err
	}

	if n.Reference, err = props.String("reference"); err != nil {
		return err
	}
	return nil
}

// UnmarshalJSON converts JSON bytes to a ReferenceNode
func (n *ReferenceNode) UnmarshalJSON(data []byte) error {
	var props JSONNode
	err := json.Unmarshal(data, &props)
	if err != nil {
		return err
	}
	return n.unmarshal(props)

}
func newReference(p position, txt string, c *CommentNode) *ReferenceNode {
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
func (n *ReferenceNode) SetComment(c *CommentNode) {
	n.Comment = c
}
func (n *ReferenceNode) Equal(o interface{}) bool {
	if on, ok := o.(*ReferenceNode); ok {
		return n.Reference == on.Reference
	}
	return false
}

//Holds the textual representation of a string literal
type StringNode struct {
	position
	Literal      string // The string literal
	TripleQuotes bool
	Comment      *CommentNode
}

// MarshalJSON converts the node to JSON with an additional
// typeOf field.
func (n *StringNode) MarshalJSON() ([]byte, error) {
	props := JSONNode{}.
		Type("string").
		Set("literal", n.Literal)

	return json.Marshal(&props)
}

func (n *StringNode) unmarshal(props JSONNode) error {
	err := props.CheckTypeOf("string")
	if err != nil {
		return err
	}

	if n.Literal, err = props.String("literal"); err != nil {
		return err
	}
	return nil
}

// UnmarshalJSON converts JSON bytes to a StringNode
func (n *StringNode) UnmarshalJSON(data []byte) error {
	var props JSONNode
	err := json.Unmarshal(data, &props)
	if err != nil {
		return err
	}
	return n.unmarshal(props)
}

func newString(p position, txt string, c *CommentNode) *StringNode {

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
func (n *StringNode) SetComment(c *CommentNode) {
	n.Comment = c
}

func (n *StringNode) Equal(o interface{}) bool {
	if on, ok := o.(*StringNode); ok {
		return n.Literal == on.Literal
	}
	return false
}

//Holds the list of other nodes
type ListNode struct {
	position
	Nodes   []Node
	Comment *CommentNode
}

// MarshalJSON converts the node to JSON with an additional
// typeOf field.
func (n *ListNode) MarshalJSON() ([]byte, error) {
	props := JSONNode{}.
		Type("list").
		Set("nodes", n.Nodes)

	return json.Marshal(&props)
}

func (n *ListNode) unmarshal(props JSONNode) error {
	err := props.CheckTypeOf("list")
	if err != nil {
		return err
	}

	if n.Nodes, err = props.NodeList("nodes"); err != nil {
		return err
	}
	return nil
}

// UnmarshalJSON converts JSON bytes to a ListNode
func (n *ListNode) UnmarshalJSON(data []byte) error {
	var props JSONNode
	err := json.Unmarshal(data, &props)
	if err != nil {
		return err
	}
	return n.unmarshal(props)
}

func newList(p position, nodes []Node, c *CommentNode) *ListNode {
	return &ListNode{
		position: p,
		Nodes:    nodes,
		Comment:  c,
	}
}

func (n *ListNode) String() string {
	return fmt.Sprintf("ListNode@%v{%v}%v", n.position, n.Nodes, n.Comment)
}

func (n *ListNode) Format(buf *bytes.Buffer, indent string, onNewLine bool) {
	if n.Comment != nil {
		n.Comment.Format(buf, indent, onNewLine)
		onNewLine = true
	}
	writeIndent(buf, indent, onNewLine)
	buf.WriteByte('[')
	for i, sn := range n.Nodes {
		if i != 0 {
			buf.WriteString(", ")
		}
		sn.Format(buf, indent, onNewLine)

	}
	buf.WriteByte(']')
}

func (n *ListNode) SetComment(c *CommentNode) {
	n.Comment = c
}

func (n *ListNode) Equal(o interface{}) bool {
	if on, ok := o.(*ListNode); ok {
		if len(n.Nodes) != len(on.Nodes) {
			return false
		}
		for i := range n.Nodes {
			if !n.Nodes[i].Equal(on.Nodes[i]) {
				return false
			}
		}
		return true
	}
	return false
}

//Holds the textual representation of a regex literal
type RegexNode struct {
	position
	Regex   *regexp.Regexp
	Literal string
	Comment *CommentNode
}

// MarshalJSON converts the node to JSON with an additional
// typeOf field.
func (n *RegexNode) MarshalJSON() ([]byte, error) {
	props := JSONNode{}.
		Type("regex")
	if n.Regex == nil && n.Literal != "" {
		props = props.Set("regex", n.Literal)
	} else {
		props = props.SetRegex("regex", n.Regex)
	}

	return json.Marshal(&props)
}

func (n *RegexNode) unmarshal(props JSONNode) error {
	err := props.CheckTypeOf("regex")
	if err != nil {
		return err
	}

	if n.Regex, err = props.Regex("regex"); err != nil {
		return err
	}
	return nil
}

// UnmarshalJSON converts JSON bytes to a RegexNode
func (n *RegexNode) UnmarshalJSON(data []byte) error {
	var props JSONNode
	err := json.Unmarshal(data, &props)
	if err != nil {
		return err
	}
	return n.unmarshal(props)
}

func newRegex(p position, txt string, c *CommentNode) (*RegexNode, error) {
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
	unescaped := buf.String()

	r, err := regexp.Compile(unescaped)
	if err != nil {
		return nil, err
	}

	return &RegexNode{
		position: p,
		Regex:    r,
		Literal:  literal,
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
	buf.WriteString(n.Literal)
	buf.WriteByte('/')
}

func (n *RegexNode) SetComment(c *CommentNode) {
	n.Comment = c
}
func (n *RegexNode) Equal(o interface{}) bool {
	if on, ok := o.(*RegexNode); ok {
		return n.Regex.String() == on.Regex.String()
	}
	return false
}

// Represents a standalone '*' token.
type StarNode struct {
	position
	Comment *CommentNode
}

// MarshalJSON converts the node to JSON with an additional
// typeOf field.
func (n *StarNode) MarshalJSON() ([]byte, error) {
	props := JSONNode{}.
		Type("star")

	return json.Marshal(&props)
}

func (n *StarNode) unmarshal(props JSONNode) error {
	err := props.CheckTypeOf("star")
	if err != nil {
		return err
	}
	return nil
}

// UnmarshalJSON converts JSON bytes to a StarNode
func (n *StarNode) UnmarshalJSON(data []byte) error {
	var props JSONNode
	err := json.Unmarshal(data, &props)
	if err != nil {
		return err
	}
	return n.unmarshal(props)
}

func newStar(p position, c *CommentNode) *StarNode {
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
func (n *StarNode) SetComment(c *CommentNode) {
	n.Comment = c
}

func (n *StarNode) Equal(o interface{}) bool {
	_, ok := o.(*StarNode)
	return ok
}

type FuncType int

const (
	GlobalFunc FuncType = iota
	ChainFunc
	PropertyFunc
	DynamicFunc
)

func (ft FuncType) String() string {
	switch ft {
	case GlobalFunc:
		return "global"
	case ChainFunc:
		return "chain"
	case PropertyFunc:
		return "property"
	case DynamicFunc:
		return "dynamic"
	default:
		return "unknown"
	}
}

func NewFuncType(typ string) (FuncType, error) {
	switch typ {
	case "global":
		return GlobalFunc, nil
	case "chain":
		return ChainFunc, nil
	case "property":
		return PropertyFunc, nil
	case "dynamic":
		return DynamicFunc, nil
	default:
		return -1, fmt.Errorf("unknown function type %s", typ)
	}
}

//Holds the a function call with its args
type FunctionNode struct {
	position
	Type      FuncType
	Func      string // The identifier
	Args      []Node
	Comment   *CommentNode
	MultiLine bool
}

// MarshalJSON converts the node to JSON with an additional
// typeOf field.
func (n *FunctionNode) MarshalJSON() ([]byte, error) {
	props := JSONNode{}.
		Type("func").
		SetFunctionType("functionType", n.Type).
		Set("args", n.Args)
	return json.Marshal(&props)
}

func (n *FunctionNode) unmarshal(props JSONNode) error {
	err := props.CheckTypeOf("func")
	if err != nil {
		return err
	}

	if n.Args, err = props.NodeList("args"); err != nil {
		return err
	}

	if n.Type, err = props.FunctionType("functionType"); err != nil {
		return err
	}

	return nil
}

// UnmarshalJSON converts JSON bytes to a FunctionNode
func (n *FunctionNode) UnmarshalJSON(data []byte) error {
	var props JSONNode
	err := json.Unmarshal(data, &props)
	if err != nil {
		return err
	}
	return n.unmarshal(props)
}

func newFunc(p position, ft FuncType, ident string, args []Node, multi bool, c *CommentNode) *FunctionNode {
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
func (n *FunctionNode) SetComment(c *CommentNode) {
	n.Comment = c
}
func (n *FunctionNode) Equal(o interface{}) bool {
	if on, ok := o.(*FunctionNode); ok {
		if n.Type != on.Type || n.Func != on.Func || len(n.Args) != len(on.Args) {
			return false
		}
		for i := range n.Args {
			if !n.Args[i].Equal(on.Args[i]) {
				return false
			}
		}
		return true
	}
	return false
}

// Represents the beginning of a lambda expression
type LambdaNode struct {
	position
	Expression Node
	Comment    *CommentNode
}

// MarshalJSON converts the node to JSON with an additional
// typeOf field.
func (n *LambdaNode) MarshalJSON() ([]byte, error) {
	props := JSONNode{}.
		Type("lambda").
		Set("expression", n.Expression)
	return json.Marshal(&props)
}

// Unmarshal deserializes the props map into this LambdaNode
func (n *LambdaNode) Unmarshal(props map[string]interface{}) error {
	return n.unmarshal(JSONNode(props))
}

func (n *LambdaNode) unmarshal(props JSONNode) error {
	err := props.CheckTypeOf("lambda")
	if err != nil {
		return err
	}

	if n.Expression, err = props.Node("expression"); err != nil {
		return err
	}
	return nil
}

// UnmarshalJSON converts JSON bytes to a LambdaNode
func (n *LambdaNode) UnmarshalJSON(data []byte) error {
	var props JSONNode
	err := json.Unmarshal(data, &props)
	if err != nil {
		return err
	}
	return n.unmarshal(props)
}

func newLambda(p position, node Node, c *CommentNode) *LambdaNode {
	return &LambdaNode{
		position:   p,
		Expression: node,
		Comment:    c,
	}
}

func (n *LambdaNode) String() string {
	return fmt.Sprintf("LambdaNode@%v{%v}%v", n.position, n.Expression, n.Comment)
}

func (n *LambdaNode) Format(buf *bytes.Buffer, indent string, onNewLine bool) {
	if n.Comment != nil {
		n.Comment.Format(buf, indent, onNewLine)
		onNewLine = true
	}
	writeIndent(buf, indent, onNewLine)
	buf.WriteString("lambda: ")
	n.Expression.Format(buf, indent, false)
}
func (n *LambdaNode) SetComment(c *CommentNode) {
	n.Comment = c
}
func (n *LambdaNode) Equal(o interface{}) bool {
	if on, ok := o.(*LambdaNode); ok {
		return (n == nil && on == nil) || n.Expression.Equal(on.Expression)
	}
	return false
}

func (n *LambdaNode) ExpressionString() string {
	if n.Expression == nil {
		return ""
	}
	var buf bytes.Buffer
	n.Expression.Format(&buf, "", true)
	return buf.String()
}

//Holds a function call with its args
type ProgramNode struct {
	position
	Nodes []Node
}

// MarshalJSON converts the node to JSON with an additional
// typeOf field.
func (n *ProgramNode) MarshalJSON() ([]byte, error) {
	props := JSONNode{}.
		Type("program").
		Set("nodes", n.Nodes)

	return json.Marshal(&props)
}

func (n *ProgramNode) unmarshal(props JSONNode) error {
	err := props.CheckTypeOf("program")
	if err != nil {
		return err
	}

	if n.Nodes, err = props.NodeList("nodes"); err != nil {
		return err
	}
	return nil
}

// UnmarshalJSON converts JSON bytes to a ProgramNode
func (n *ProgramNode) UnmarshalJSON(data []byte) error {
	var props JSONNode
	err := json.Unmarshal(data, &props)
	if err != nil {
		return err
	}
	return n.unmarshal(props)
}

func newProgram(p position) *ProgramNode {
	return &ProgramNode{
		position: p,
	}
}

func (n *ProgramNode) Add(node Node) {
	n.Nodes = append(n.Nodes, node)
}

func (n *ProgramNode) String() string {
	return fmt.Sprintf("ProgramNode@%v{%v}", n.position, n.Nodes)
}

func (n *ProgramNode) Format(buf *bytes.Buffer, indent string, onNewLine bool) {
	for i, node := range n.Nodes {
		if i != 0 {
			buf.WriteByte('\n')
		}
		node.Format(buf, indent, true)
		buf.WriteByte('\n')
	}
}

func (n *ProgramNode) Equal(o interface{}) bool {
	if on, ok := o.(*ProgramNode); ok {
		if len(n.Nodes) != len(on.Nodes) {
			return false
		}
		for i := range n.Nodes {
			if !n.Nodes[i].Equal(on.Nodes[i]) {
				return false
			}
		}
		return true
	}
	return false
}

// Hold the contents of a comment
type CommentNode struct {
	position
	Comments []string
}

// MarshalJSON converts the node to JSON with an additional
// typeOf field.
func (n *CommentNode) MarshalJSON() ([]byte, error) {
	props := JSONNode{}.
		Type("comment").
		Set("comments", n.Comments)

	return json.Marshal(&props)
}

func (n *CommentNode) unmarshal(props JSONNode) error {
	err := props.CheckTypeOf("comment")
	if err != nil {
		return err
	}

	if n.Comments, err = props.Strings("comments"); err != nil {
		return err
	}
	return nil
}

// UnmarshalJSON converts JSON bytes to a CommentNode
func (n *CommentNode) UnmarshalJSON(data []byte) error {
	var props JSONNode
	err := json.Unmarshal(data, &props)
	if err != nil {
		return err
	}
	return n.unmarshal(props)
}

func newComment(p position, commentTokens []string) *CommentNode {
	allLines := make([]string, 0, len(commentTokens))
	for i, commentToken := range commentTokens {
		if i != 0 {
			allLines = append(allLines, "\n")
		}
		comments := strings.Split(commentToken, "\n")
		for _, comment := range comments {
			comment = strings.TrimSpace(comment)
			if comment == "" {
				continue
			}
			line := strings.TrimPrefix(
				strings.TrimPrefix(
					comment,
					"//",
				),
				" ",
			)
			allLines = append(allLines, line)
		}
	}
	return &CommentNode{
		position: p,
		Comments: allLines,
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
		if comment == "\n" {
			buf.WriteByte('\n')
			continue
		}
		buf.WriteString(indent)
		buf.WriteString("//")
		if len(comment) > 0 {
			buf.WriteByte(' ')
			buf.WriteString(comment)
		}
		buf.WriteByte('\n')
	}
}

func (n *CommentNode) CommentString() string {
	return strings.Join(n.Comments, "\n")
}

func (n *CommentNode) Equal(o interface{}) bool {
	// We only care about functional equivalence so actual comment contents do not matter.
	_, ok := o.(*CommentNode)
	return ok
}
