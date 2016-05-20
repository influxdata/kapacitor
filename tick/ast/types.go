package ast

import (
	"errors"
	"fmt"
	"regexp"
	"time"
)

type ValueType uint8

const (
	InvalidType ValueType = iota << 1
	TFloat
	TInt
	TString
	TBool
	TRegex
	TTime
	TDuration
	TLambda
	TList
	TStar
)

func (v ValueType) String() string {
	switch v {
	case TFloat:
		return "float"
	case TInt:
		return "int"
	case TString:
		return "string"
	case TBool:
		return "boolean"
	case TRegex:
		return "regex"
	case TTime:
		return "time"
	case TDuration:
		return "duration"
	case TLambda:
		return "lambda"
	case TList:
		return "list"
	case TStar:
		return "star"
	}

	return "invalid type"
}

func TypeOf(v interface{}) ValueType {
	switch v.(type) {
	case float64:
		return TFloat
	case int64:
		return TInt
	case string:
		return TString
	case bool:
		return TBool
	case *regexp.Regexp:
		return TRegex
	case time.Time:
		return TTime
	case time.Duration:
		return TDuration
	case *LambdaNode:
		return TLambda
	case []interface{}:
		return TList
	case *StarNode:
		return TStar
	default:
		return InvalidType
	}
}

func ZeroValue(t ValueType) interface{} {
	switch t {
	case TFloat:
		return float64(0)
	case TInt:
		return int64(0)
	case TString:
		return ""
	case TBool:
		return false
	case TRegex:
		return (*regexp.Regexp)(nil)
	case TTime:
		return time.Time{}
	case TDuration:
		return time.Duration(0)
	case TLambda:
		return (*LambdaNode)(nil)
	case TList:
		return []interface{}(nil)
	case TStar:
		return (*StarNode)(nil)
	default:
		return errors.New("invalid type")
	}
}

// Convert raw value to literal node, for all supported basic types.
func ValueToLiteralNode(pos Position, v interface{}) (Node, error) {
	p := position{
		pos:  pos.Position(),
		line: pos.Line(),
		char: pos.Char(),
	}
	switch value := v.(type) {
	case bool:
		return &BoolNode{
			position: p,
			Bool:     value,
		}, nil
	case int64:
		return &NumberNode{
			position: p,
			IsInt:    true,
			Int64:    value,
		}, nil
	case float64:
		return &NumberNode{
			position: p,
			IsFloat:  true,
			Float64:  value,
		}, nil
	case time.Duration:
		return &DurationNode{
			position: p,
			Dur:      value,
		}, nil
	case string:
		return &StringNode{
			position: p,
			Literal:  value,
		}, nil
	case *regexp.Regexp:
		return &RegexNode{
			position: p,
			Regex:    value,
		}, nil
	case *LambdaNode:
		var e Node
		if value != nil {
			e = value.Expression
		}
		return &LambdaNode{
			position:   p,
			Expression: e,
		}, nil
	case []interface{}:
		nodes := make([]Node, len(value))
		var err error
		for i, v := range value {
			nodes[i], err = ValueToLiteralNode(pos, v)
			if err != nil {
				return nil, err
			}
		}
		return &ListNode{
			position: p,
			Nodes:    nodes,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported literal type %T", v)
	}
}
