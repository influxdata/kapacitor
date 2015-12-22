// A reflection based evaluation of an AST.
package tick

import (
	"fmt"
	"reflect"
	"regexp"
	"runtime"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// Parse and evaluate a given script for the scope.
// This evaluation method uses reflection to call
// methods on objects within the scope.
func Evaluate(script string, scope *Scope) (err error) {
	defer func(errP *error) {
		r := recover()
		if r == ErrEmptyStack {
			trace := make([]byte, 1024)
			n := runtime.Stack(trace, false)
			*errP = fmt.Errorf("evaluation caused stack error: %v Go Trace: %s", r, string(trace[:n]))
		} else if r != nil {
			panic(r)
		}

	}(&err)

	root, err := parse(script)
	if err != nil {
		return err
	}

	// Use a stack machine to evaluate the AST
	stck := &stack{}
	return eval(root, scope, stck)
}

// Evaluate a node using a stack machine in a given scope
func eval(n Node, scope *Scope, stck *stack) (err error) {
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
		err = eval(node.Node, scope, stck)
		if err != nil {
			return
		}
		evalUnary(node.Operator, scope, stck)
	case *LambdaNode:
		// Catch panic from resolveIdents and return as error.
		err = func() (e error) {
			defer func(ep *error) {
				err := recover()
				if err != nil {
					*ep = err.(error)
				}
			}(&e)
			node.Node = resolveIdents(node.Node, scope)
			return e
		}()
		if err != nil {
			return
		}
		stck.Push(node.Node)
	case *BinaryNode:
		err = eval(node.Left, scope, stck)
		if err != nil {
			return
		}
		err = eval(node.Right, scope, stck)
		if err != nil {
			return
		}
		err = evalBinary(node.Operator, scope, stck)
		if err != nil {
			return
		}
	case *FunctionNode:
		args := make([]reflect.Value, len(node.Args))
		for i, arg := range node.Args {
			err = eval(arg, scope, stck)
			if err != nil {
				return
			}
			a := stck.Pop()
			switch typed := a.(type) {
			case *IdentifierNode:
				// Resolve identifier
				a, err = scope.Get(typed.Ident)
				if err != nil {
					return err
				}
			case unboundFunc:
				// Call global func
				a, err = typed(nil)
				if err != nil {
					return err
				}
			}

			args[i] = reflect.ValueOf(a)
		}
		err = evalFunc(node, scope, stck, args)
		if err != nil {
			return
		}
	case *ListNode:
		for _, n := range node.Nodes {
			err = eval(n, scope, stck)
			if err != nil {
				return
			}
			// Pop unused result
			if stck.Len() > 0 {
				stck.Pop()
			}
		}
	default:
		stck.Push(node)
	}
	return nil
}

func evalUnary(op tokenType, scope *Scope, stck *stack) error {
	v := stck.Pop()
	switch op {
	case tokenMinus:
		switch n := v.(type) {
		case float64:
			stck.Push(-1 * n)
		case int64:
			stck.Push(-1 * n)
		default:
			return fmt.Errorf("invalid arugument to '-' %v", v)
		}
	case tokenNot:
		if b, ok := v.(bool); ok {
			stck.Push(!b)
		} else {
			return fmt.Errorf("invalid arugument to '!' %v", v)
		}
	}
	return nil
}

func evalBinary(op tokenType, scope *Scope, stck *stack) error {
	r := stck.Pop()
	l := stck.Pop()
	switch op {
	case tokenAsgn:
		i := l.(*IdentifierNode)
		scope.Set(i.Ident, r)
	case tokenDot:
		// Resolve identifier
		if left, ok := l.(*IdentifierNode); ok {
			var err error
			l, err = scope.Get(left.Ident)
			if err != nil {
				return err
			}
		}
		switch right := r.(type) {
		case unboundFunc:
			ret, err := right(l)
			if err != nil {
				return err
			}
			stck.Push(ret)
		case *IdentifierNode:
			name := capilatizeFirst(right.Ident)

			//Lookup field by name of left object
			v := reflect.ValueOf(l)
			if !v.IsValid() {
				return fmt.Errorf("object is not valid, cannot get field %s of %v", name, l)
			}
			v = reflect.Indirect(v)
			if v.Kind() == reflect.Struct {
				field := v.FieldByName(name)
				if field.IsValid() {
					stck.Push(field.Interface())
					break
				}
			}
			return fmt.Errorf("unknown field %s of obj %T", name, l)
		default:
			return fmt.Errorf("invalid right operand of type %T to '.' operator", r)
		}
	}
	return nil
}

func evalFunc(f *FunctionNode, scope *Scope, stck *stack, args []reflect.Value) error {
	rec := func(obj interface{}, errp *error) {
		e := recover()
		if e != nil {
			*errp = fmt.Errorf("error calling func %q on obj %T: %v", f.Func, obj, e)
			if strings.Contains((*errp).Error(), "*tick.ReferenceNode") && strings.Contains((*errp).Error(), "type string") {
				*errp = fmt.Errorf("cannot assign *tick.ReferenceNode to type string, did you use double quotes instead of single quotes?")
			}

		}
	}
	fnc := unboundFunc(func(obj interface{}) (_ interface{}, err error) {
		//Setup recover method if there is a panic during reflection
		defer rec(obj, &err)
		name := capilatizeFirst(f.Func)
		// Check for method
		var method reflect.Value
		if obj == nil {
			// Object is nil, check for func in scope
			fnc, _ := scope.Get(f.Func)
			if fnc == nil {
				return nil, fmt.Errorf("no global function %q defined", f.Func)
			}
			method = reflect.ValueOf(fnc)
		} else {
			v := reflect.ValueOf(obj)
			if !v.IsValid() {
				return nil, fmt.Errorf("error calling %q on object %T", f.Func, obj)
			}
			method = v.MethodByName(name)
		}
		if method.IsValid() {
			ret := method.Call(args)
			if l := len(ret); l == 1 {
				return ret[0].Interface(), nil
			} else if l == 2 {
				if i := ret[1].Interface(); i != nil {
					if err, ok := i.(error); !ok {
						return nil, fmt.Errorf("second return value form function must be an 'error', got %T", i)
					} else {
						return nil, err
					}
				} else {
					return ret[0].Interface(), nil
				}
			} else {
				return nil, fmt.Errorf("functions must return a single value or (interface{}, error)")
			}
		}

		// Check for settable field
		v := reflect.Indirect(reflect.ValueOf(obj))
		if len(f.Args) == 1 && v.Kind() == reflect.Struct {
			field := v.FieldByName(name)
			if field.IsValid() && field.CanSet() {
				field.Set(args[0])
				return obj, nil
			}
		}
		return nil, fmt.Errorf("No method or field %q on %T", name, obj)
	})
	stck.Push(fnc)
	return nil
}

// Capilatizes the first rune in the string
func capilatizeFirst(s string) string {
	r, n := utf8.DecodeRuneInString(s)
	s = string(unicode.ToUpper(r)) + s[n:]
	return s
}

// Resolve all identifiers immediately in the tree with their value from the scope.
// This operation is performed in place.
// Panics if the scope value does not exist or if the value cannot be expressed as a literal.
func resolveIdents(n Node, scope *Scope) Node {
	switch node := n.(type) {
	case *IdentifierNode:
		v, err := scope.Get(node.Ident)
		if err != nil {
			panic(err)
		}
		return valueToLiteralNode(node.pos, v)
	case *UnaryNode:
		node.Node = resolveIdents(node.Node, scope)
	case *BinaryNode:
		node.Left = resolveIdents(node.Left, scope)
		node.Right = resolveIdents(node.Right, scope)
	case *FunctionNode:
		for i, arg := range node.Args {
			node.Args[i] = resolveIdents(arg, scope)
		}
	case *ListNode:
		for i, n := range node.Nodes {
			node.Nodes[i] = resolveIdents(n, scope)
		}
	}
	return n
}

// Convert raw value to literal node, for all supported basic types.
func valueToLiteralNode(pos pos, v interface{}) Node {
	switch value := v.(type) {
	case bool:
		return &BoolNode{
			pos:  pos,
			Bool: value,
		}
	case int64:
		return &NumberNode{
			pos:   pos,
			IsInt: true,
			Int64: value,
		}
	case float64:
		return &NumberNode{
			pos:     pos,
			IsFloat: true,
			Float64: value,
		}
	case time.Duration:
		return &DurationNode{
			pos: pos,
			Dur: value,
		}
	case string:
		return &StringNode{
			pos:     pos,
			Literal: value,
		}
	case *regexp.Regexp:
		return &RegexNode{
			pos:   pos,
			Regex: value,
		}
	default:
		panic(fmt.Errorf("unsupported literal type %T", v))
	}
}
