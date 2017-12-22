package ast

import (
	"fmt"
	"regexp"
	"time"

	"github.com/influxdata/influxdb/influxql"
)

// NodeTypeOf is used by all Node to identify the node duration Marshal and Unmarshal
const NodeTypeOf = "typeOf"

// JSONNode is the intermediate type between Node and JSON serialization
type JSONNode map[string]interface{}

// Type adds the Node type information
func (j JSONNode) Type(typ string) JSONNode {
	j[NodeTypeOf] = typ
	return j
}

// Set adds the key/value to the JSONNode
func (j JSONNode) Set(key string, value interface{}) JSONNode {
	j[key] = value
	return j
}

// SetDuration adds key to the JSONNode but formats the duration in InfluxQL style
func (j JSONNode) SetDuration(key string, value time.Duration) JSONNode {
	return j.Set(key, influxql.FormatDuration(value))
}

// SetRegex adds key to the JSONNode but formats the regex as a string
func (j JSONNode) SetRegex(key string, value *regexp.Regexp) JSONNode {
	if value == nil {
		return j.Set(key, nil)
	}
	return j.Set(key, value.String())
}

// SetOperator adds key to JSONNode but formats the operator as a string
func (j JSONNode) SetOperator(key string, op TokenType) JSONNode {
	return j.Set(key, op.String())
}

// SetFunctionType adds key to JSONNode but formats the function type as a string
func (j JSONNode) SetFunctionType(key string, fn FuncType) JSONNode {
	return j.Set(key, fn.String())
}

// TypeOf returns the type of the node
func (j JSONNode) TypeOf() (string, error) {
	return j.String(NodeTypeOf)
}

// CheckTypeOf tests that the typeOf field is correctly set to typ.
func (j JSONNode) CheckTypeOf(typ string) error {
	t, ok := j[NodeTypeOf]
	if !ok {
		return fmt.Errorf("missing typeOf field")
	}

	if t != typ {
		return fmt.Errorf("error unmarshaling node type %s; received %s", typ, t)
	}
	return nil
}

// Has returns true if field exists
func (j JSONNode) Has(field string) bool {
	_, ok := j[field]
	return ok
}

// Field returns expected field or error if field doesn't exist
func (j JSONNode) Field(field string) (interface{}, error) {
	fld, ok := j[field]
	if !ok {
		return nil, fmt.Errorf("missing expected field %s", field)
	}
	return fld, nil
}

// String reads the field for a string value
func (j JSONNode) String(field string) (string, error) {
	s, err := j.Field(field)
	if err != nil {
		return "", err
	}

	str, ok := s.(string)
	if !ok {
		return "", fmt.Errorf("field %s is not a string value but is %T", field, s)
	}
	return str, nil
}

// Int64 reads the field for a int64 value
func (j JSONNode) Int64(field string) (int64, error) {
	n, err := j.Field(field)
	if err != nil {
		return 0, err
	}

	num, ok := n.(int64)
	if !ok {
		flt, ok := n.(float64)
		if !ok {
			return 0, fmt.Errorf("field %s is not an integer value but is %T", field, n)
		}
		num = int64(flt)
	}
	return num, nil
}

// Float64 reads the field for a float64 value
func (j JSONNode) Float64(field string) (float64, error) {
	n, err := j.Field(field)
	if err != nil {
		return 0, err
	}

	num, ok := n.(float64)
	if !ok {
		integer, ok := n.(int64)
		if !ok {
			return 0, fmt.Errorf("field %s is not a floating point value but is %T", field, n)
		}
		num = float64(integer)
	}
	return num, nil
}

// Strings reads the field an array of strings
func (j JSONNode) Strings(field string) ([]string, error) {
	s, err := j.Field(field)
	if err != nil {
		return nil, err
	}

	strs, ok := s.([]string)
	if !ok {
		return nil, fmt.Errorf("field %s is not an array of strings but is %T", field, s)
	}
	return strs, nil
}

// Duration reads the field and assumes the string is in InfluxQL Duration format.
func (j JSONNode) Duration(field string) (time.Duration, error) {
	d, err := j.Field(field)
	if err != nil {
		return 0, err
	}

	dur, ok := d.(string)
	if !ok {
		return 0, fmt.Errorf("field %s is not a string duration value but is %T", field, d)
	}

	return influxql.ParseDuration(dur)
}

// Regex reads the field and assumes the string is a regular expression.
func (j JSONNode) Regex(field string) (*regexp.Regexp, error) {
	r, err := j.Field(field)
	if err != nil {
		return nil, err
	}

	re, ok := r.(string)
	if !ok {
		return nil, fmt.Errorf("field %s is not a string regex value but is %T", field, r)
	}

	return regexp.Compile(re)
}

// Bool reads the field for a boolean value
func (j JSONNode) Bool(field string) (bool, error) {
	b, err := j.Field(field)
	if err != nil {
		return false, err
	}

	boolean, ok := b.(bool)
	if !ok {
		return false, fmt.Errorf("field %s is not a bool value but is %T", field, b)
	}
	return boolean, nil
}

// Operator reads the field for an TokenType operator value
func (j JSONNode) Operator(field string) (TokenType, error) {
	o, err := j.Field(field)
	if err != nil {
		return TokenError, err
	}

	op, ok := o.(string)
	if !ok {
		return TokenError, fmt.Errorf("field %s is not an operator value but is %T", field, o)
	}

	return NewTokenType(op)
}

// FunctionType reads the field for an FuncType value
func (j JSONNode) FunctionType(field string) (FuncType, error) {
	f, err := j.Field(field)
	if err != nil {
		return -1, err
	}

	fn, ok := f.(string)
	if !ok {
		return -1, fmt.Errorf("field %s is not a function type value but is %T", field, f)
	}

	return NewFuncType(fn)
}

// NodeList reads the field for a list of nodes
func (j JSONNode) NodeList(field string) ([]Node, error) {
	l, err := j.Field(field)
	if err != nil {
		return nil, err
	}
	list, ok := l.([]interface{})
	if !ok {
		return nil, fmt.Errorf("field %s is not a list of values but is %T", field, l)
	}
	nodes := make([]Node, len(list))
	for i, lst := range list {
		nodes[i], err = j.getNode(lst)
		if err != nil {
			return nil, err
		}
	}

	return nodes, nil
}

// Node reads the field for a node
func (j JSONNode) Node(field string) (Node, error) {
	nn, err := j.Field(field)
	if err != nil {
		return nil, err
	}
	return j.getNode(nn)
}

// IDNode reads an IdentifierNode from the field
func (j JSONNode) IDNode(field string) (*IdentifierNode, error) {
	n, err := j.Node(field)
	if err != nil {
		return nil, err
	}
	id, ok := n.(*IdentifierNode)
	if !ok {
		return nil, fmt.Errorf("field %s is not an identifier node but is %T", field, n)
	}
	return id, nil
}

// RefNode reads a ReferenceNode from the field
func (j JSONNode) RefNode(field string) (*ReferenceNode, error) {
	n, err := j.Node(field)
	if err != nil {
		return nil, err
	}
	id, ok := n.(*ReferenceNode)
	if !ok {
		return nil, fmt.Errorf("field %s is not a reference node but is %T", field, n)
	}
	return id, nil
}

func (j JSONNode) getNode(nn interface{}) (Node, error) {
	nd, ok := nn.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("expected node type but is %T", nn)
	}

	node := JSONNode(nd)
	typ, err := node.TypeOf()
	if err != nil {
		return nil, err
	}

	var n Node
	switch typ {
	case "number":
		n = &NumberNode{}
	case "dbrp":
		n = &DBRPNode{}
	case "duration":
		n = &DurationNode{}
	case "bool":
		n = &BoolNode{}
	case "unary":
		n = &UnaryNode{}
	case "binary":
		n = &BinaryNode{}
	case "declaration":
		n = &DeclarationNode{}
	case "typeDeclaration":
		n = &TypeDeclarationNode{}
	case "identifier":
		n = &IdentifierNode{}
	case "reference":
		n = &ReferenceNode{}
	case "string":
		n = &StringNode{}
	case "list":
		n = &ListNode{}
	case "regex":
		n = &RegexNode{}
	case "star":
		n = &StarNode{}
	case "func":
		n = &FunctionNode{}
	case "lambda":
		n = &LambdaNode{}
	case "program":
		n = &ProgramNode{}
	case "comment":
		n = &CommentNode{}
	}
	err = n.unmarshal(node)
	return n, err
}
