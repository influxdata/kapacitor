package tick

import (
	"errors"
	"fmt"
	"math"
	"regexp"
)

var ErrInvalidExpr = errors.New("expression is invalid, could not evaluate")

// Expression functions are stateful. Their state is updated with
// each call to the function. A StatefulExpr is a Node
// and its associated function state.
type StatefulExpr struct {
	Node  Node
	Funcs Funcs
}

func NewStatefulExpr(n Node) *StatefulExpr {
	return &StatefulExpr{
		Node:  n,
		Funcs: NewFunctions(),
	}
}

// Reset the state
func (s *StatefulExpr) Reset() {
	for _, f := range s.Funcs {
		f.Reset()
	}
}

func (s *StatefulExpr) EvalBool(scope *Scope) (bool, error) {
	stck := &stack{}
	err := s.eval(s.Node, scope, stck)
	if err != nil {
		return false, err
	}
	if stck.Len() == 1 {
		value := stck.Pop()
		// Resolve reference
		if ref, ok := value.(*ReferenceNode); ok {
			value, err = scope.Get(ref.Reference)
			if err != nil {
				return false, err
			}
		}
		b, ok := value.(bool)
		if ok {
			return b, nil
		} else {
			return false, fmt.Errorf("expression returned unexpected type %T", value)
		}
	}
	return false, ErrInvalidExpr
}

func (s *StatefulExpr) EvalNum(scope *Scope) (interface{}, error) {
	stck := &stack{}
	err := s.eval(s.Node, scope, stck)
	if err != nil {
		return math.NaN(), err
	}
	if stck.Len() == 1 {
		value := stck.Pop()
		// Resolve reference
		if ref, ok := value.(*ReferenceNode); ok {
			value, err = scope.Get(ref.Reference)
			if err != nil {
				return math.NaN(), err
			}
		}
		switch value.(type) {
		case float64, int64:
			return value, nil
		default:
			return math.NaN(), fmt.Errorf("expression returned unexpected type %T", value)
		}
	}
	return math.NaN(), ErrInvalidExpr
}

func (s *StatefulExpr) eval(n Node, scope *Scope, stck *stack) (err error) {
	switch node := n.(type) {
	case *BoolNode:
		stck.Push(node.Bool)
	case *NumberNode:
		if node.IsInt {
			stck.Push(node.Int64)
		} else {
			stck.Push(node.Float64)
		}
	case *DurationNode:
		stck.Push(node.Dur)
	case *StringNode:
		stck.Push(node.Literal)
	case *RegexNode:
		stck.Push(node.Regex)
	case *UnaryNode:
		err = s.eval(node.Node, scope, stck)
		if err != nil {
			return
		}
		s.evalUnary(node.Operator, scope, stck)
	case *BinaryNode:
		err = s.eval(node.Left, scope, stck)
		if err != nil {
			return
		}
		err = s.eval(node.Right, scope, stck)
		if err != nil {
			return
		}
		err = s.evalBinary(node.Operator, scope, stck)
		if err != nil {
			return
		}
	case *FunctionNode:
		args := make([]interface{}, len(node.Args))
		for i, arg := range node.Args {
			err = s.eval(arg, scope, stck)
			if err != nil {
				return
			}
			a := stck.Pop()
			if r, ok := a.(*ReferenceNode); ok {
				a, err = scope.Get(r.Reference)
				if err != nil {
					return err
				}
			}
			args[i] = a
		}
		// Call function
		f := s.Funcs[node.Func]
		if f == nil {
			return fmt.Errorf("undefined function %s", node.Func)
		}
		ret, err := f.Call(args...)
		if err != nil {
			return fmt.Errorf("error calling %s: %s", node.Func, err)
		}
		stck.Push(ret)
	default:
		stck.Push(node)
	}
	return nil
}

func (s *StatefulExpr) evalUnary(op tokenType, scope *Scope, stck *stack) error {
	v := stck.Pop()
	switch op {
	case TokenMinus:
		switch n := v.(type) {
		case float64:
			stck.Push(-1 * n)
		case int64:
			stck.Push(-1 * n)
		default:
			return fmt.Errorf("invalid arugument to '-' %v", v)
		}
	case TokenNot:
		if b, ok := v.(bool); ok {
			stck.Push(!b)
		} else {
			return fmt.Errorf("invalid arugument to '!' %v", v)
		}
	}
	return nil
}

func errMismatched(op tokenType, l, r interface{}) error {
	return fmt.Errorf("mismatched type to binary operator. got %T %v %T. see bool(), int(), float()", l, op, r)
}

func (s *StatefulExpr) evalBinary(op tokenType, scope *Scope, stck *stack) (err error) {
	r := stck.Pop()
	l := stck.Pop()
	// Resolve any references
	if ref, ok := l.(*ReferenceNode); ok {
		l, err = scope.Get(ref.Reference)
		if err != nil {
			return err
		}
	}
	if ref, ok := r.(*ReferenceNode); ok {
		r, err = scope.Get(ref.Reference)
		if err != nil {
			return err
		}
	}
	var v interface{}
	switch {
	case isMathOperator(op):
		switch ln := l.(type) {
		case int64:
			rn, ok := r.(int64)
			if !ok {
				return errMismatched(op, l, r)
			}
			v, err = doIntMath(op, ln, rn)
		case float64:
			rn, ok := r.(float64)
			if !ok {
				return errMismatched(op, l, r)
			}
			v, err = doFloatMath(op, ln, rn)
		default:
			return errMismatched(op, l, r)
		}
	case isCompOperator(op):
		switch ln := l.(type) {
		case bool:
			rn, ok := r.(bool)
			if !ok {
				return errMismatched(op, l, r)
			}
			v, err = doBoolComp(op, ln, rn)
		case int64:
			lf := float64(ln)
			var rf float64
			switch rn := r.(type) {
			case int64:
				rf = float64(rn)
			case float64:
				rf = rn
			default:
				return errMismatched(op, l, r)
			}
			v, err = doFloatComp(op, lf, rf)
		case float64:
			var rf float64
			switch rn := r.(type) {
			case int64:
				rf = float64(rn)
			case float64:
				rf = rn
			default:
				return errMismatched(op, l, r)
			}
			v, err = doFloatComp(op, ln, rf)
		case string:
			rn, ok := r.(string)
			if ok {
				v, err = doStringComp(op, ln, rn)
			} else if rx, ok := r.(*regexp.Regexp); ok {
				v, err = doRegexComp(op, ln, rx)
			} else {
				return errMismatched(op, l, r)
			}
		default:
			return errMismatched(op, l, r)
		}
	default:
		return fmt.Errorf("return: unknown operator %v", op)
	}
	if err != nil {
		return
	}
	stck.Push(v)
	return
}

func doIntMath(op tokenType, l, r int64) (v int64, err error) {
	switch op {
	case TokenPlus:
		v = l + r
	case TokenMinus:
		v = l - r
	case TokenMult:
		v = l * r
	case TokenDiv:
		v = l / r
	case TokenMod:
		v = l % r
	default:
		return 0, fmt.Errorf("invalid integer math operator %v", op)
	}
	return
}

func doFloatMath(op tokenType, l, r float64) (v float64, err error) {
	switch op {
	case TokenPlus:
		v = l + r
	case TokenMinus:
		v = l - r
	case TokenMult:
		v = l * r
	case TokenDiv:
		v = l / r
	default:
		return math.NaN(), fmt.Errorf("invalid float math operator %v", op)
	}
	return
}

func doBoolComp(op tokenType, l, r bool) (v bool, err error) {
	switch op {
	case TokenEqual:
		v = l == r
	case TokenNotEqual:
		v = l != r
	case TokenAnd:
		v = l && r
	case TokenOr:
		v = l || r
	default:
		err = fmt.Errorf("invalid boolean comparison operator %v", op)
	}
	return
}

func doFloatComp(op tokenType, l, r float64) (v bool, err error) {
	switch op {
	case TokenEqual:
		v = l == r
	case TokenNotEqual:
		v = l != r
	case TokenLess:
		v = l < r
	case TokenGreater:
		v = l > r
	case TokenLessEqual:
		v = l <= r
	case TokenGreaterEqual:
		v = l >= r
	default:
		err = fmt.Errorf("invalid float comparison operator %v", op)
	}
	return
}

func doStringComp(op tokenType, l, r string) (v bool, err error) {
	switch op {
	case TokenEqual:
		v = l == r
	case TokenNotEqual:
		v = l != r
	case TokenLess:
		v = l < r
	case TokenGreater:
		v = l > r
	case TokenLessEqual:
		v = l <= r
	case TokenGreaterEqual:
		v = l >= r
	default:
		err = fmt.Errorf("invalid string comparison operator %v", op)
	}
	return
}

func doRegexComp(op tokenType, l string, r *regexp.Regexp) (v bool, err error) {
	switch op {
	case TokenRegexEqual:
		v = r.MatchString(l)
	case TokenRegexNotEqual:
		v = !r.MatchString(l)
	default:
		err = fmt.Errorf("invalid regex comparison operator %v", op)
	}
	return
}
