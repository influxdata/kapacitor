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

// Interface for interacting with objects.
// If an object does not self describe via this interface
// than a reflection based implemenation will be used.
type SelfDescriber interface {
	//A description the object
	Desc() string

	HasMethod(name string) bool
	CallMethod(name string, args ...interface{}) (interface{}, error)

	HasProperty(name string) bool
	Property(name string) interface{}
	SetProperty(name string, arg interface{}) error
}

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
		err := evalUnary(node.Operator, scope, stck)
		if err != nil {
			return err
		}
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
		args := make([]interface{}, len(node.Args))
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

			args[i] = a
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
	case TokenMinus:
		if ident, ok := v.(*IdentifierNode); ok {
			value, err := scope.Get(ident.Ident)
			if err != nil {
				return err
			}
			v = value
		}
		switch n := v.(type) {
		case float64:
			stck.Push(-1 * n)
		case int64:
			stck.Push(-1 * n)
		case time.Duration:
			stck.Push(-1 * n)
		default:
			return fmt.Errorf("invalid arugument to '-' %v", v)
		}
	case TokenNot:
		if ident, ok := v.(*IdentifierNode); ok {
			value, err := scope.Get(ident.Ident)
			if err != nil {
				return err
			}
			v = value
		}
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
	case TokenAsgn:
		i := l.(*IdentifierNode)
		scope.Set(i.Ident, r)
	case TokenDot:
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
			name := right.Ident

			//Lookup field by name of left object
			var describer SelfDescriber
			if d, ok := l.(SelfDescriber); ok {
				describer = d
			} else {
				describer = NewReflectionDescriber(l)
			}
			if describer.HasProperty(name) {
				stck.Push(describer.Property(name))
			} else {
				return fmt.Errorf("object %T has no property %s", l, name)
			}
		default:
			return fmt.Errorf("invalid right operand of type %T to '.' operator", r)
		}
	}
	return nil
}

func evalFunc(f *FunctionNode, scope *Scope, stck *stack, args []interface{}) error {
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
		//Setup recover method if there is a panic during the method call
		defer rec(obj, &err)

		if obj == nil {
			// Object is nil, check for func in scope
			fnc, _ := scope.Get(f.Func)
			if fnc == nil {
				return nil, fmt.Errorf("no global function %q defined", f.Func)
			}
			method := reflect.ValueOf(fnc)
			return callMethodReflection(method, args)
		}

		// Get SelfDescriber
		name := f.Func
		var describer SelfDescriber
		if d, ok := obj.(SelfDescriber); ok {
			describer = d
		} else {
			describer = NewReflectionDescriber(obj)
		}

		// Check for Method
		if describer.HasMethod(name) {
			return describer.CallMethod(name, args...)
		}

		// Check for dynamic method.
		dm := scope.DynamicMethod(name)
		if dm != nil {
			ret, err := dm(obj, args...)
			if err != nil {
				return nil, err
			}
			return ret, nil
		}

		// Ran out of options...
		return nil, fmt.Errorf("No method or property %q on %s", name, describer.Desc())
	})
	stck.Push(fnc)
	return nil
}

// Wraps any object as a SelfDescriber using reflection.
type ReflectionDescriber struct {
	obj interface{}
}

func NewReflectionDescriber(obj interface{}) *ReflectionDescriber {
	return &ReflectionDescriber{obj: obj}
}

func (r *ReflectionDescriber) Desc() string {
	return fmt.Sprintf("%T", r.obj)
}

// Using reflection check if the object has the method or field.
// A field is a valid method because we can set it via reflection too.
func (r *ReflectionDescriber) HasMethod(name string) bool {
	name = capilatizeFirst(name)
	v := reflect.ValueOf(r.obj)
	if !v.IsValid() {
		return false
	}
	if v.MethodByName(name).IsValid() {
		return true
	}
	// Check for a field of the same name,
	// we can wrap setting it in a method.
	return r.HasProperty(name)
}

func (r *ReflectionDescriber) CallMethod(name string, args ...interface{}) (interface{}, error) {
	name = capilatizeFirst(name)
	v := reflect.ValueOf(r.obj)
	if !v.IsValid() {
		return nil, fmt.Errorf("cannot get reflect.ValueOf %T", r.obj)
	}

	// Check for a method and call it
	if method := v.MethodByName(name); method.IsValid() {
		return callMethodReflection(method, args)
	}

	// Check for a field and set it
	if len(args) == 1 && r.HasProperty(name) {
		err := r.SetProperty(name, args[0])
		if err != nil {
			return nil, err
		}
		return r.obj, nil
	}
	return nil, fmt.Errorf("unknown method or field %s on %T", name, r.obj)
}

// Using reflection check if the object has a field with the property name.
func (r *ReflectionDescriber) HasProperty(name string) bool {
	name = capilatizeFirst(name)
	v := reflect.Indirect(reflect.ValueOf(r.obj))
	if v.Kind() == reflect.Struct {
		field := v.FieldByName(name)
		return field.IsValid() && field.CanSet()
	}
	return false
}

func (r *ReflectionDescriber) Property(name string) interface{} {
	name = capilatizeFirst(name)
	v := reflect.Indirect(reflect.ValueOf(r.obj))
	if v.Kind() == reflect.Struct {
		field := v.FieldByName(name)
		if field.IsValid() {
			return field.Interface()
		}
	}
	return nil
}

func (r *ReflectionDescriber) SetProperty(name string, value interface{}) error {
	v := reflect.Indirect(reflect.ValueOf(r.obj))
	if v.Kind() == reflect.Struct {
		field := v.FieldByName(name)
		if field.IsValid() && field.CanSet() {
			field.Set(reflect.ValueOf(value))
			return nil
		}
	}
	return fmt.Errorf("no field %s on %T", name, r.obj)
}

func callMethodReflection(method reflect.Value, args []interface{}) (interface{}, error) {
	rargs := make([]reflect.Value, len(args))
	for i, arg := range args {
		rargs[i] = reflect.ValueOf(arg)
	}
	ret := method.Call(rargs)
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
