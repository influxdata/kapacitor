package tick

import (
	"fmt"
	"log"
	"os"
	"reflect"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
)

var mu sync.Mutex
var logger = log.New(os.Stderr, "[tick] ", log.LstdFlags)

func getLogger() *log.Logger {
	mu.Lock()
	defer mu.Unlock()
	return logger
}
func SetLogger(l *log.Logger) {
	mu.Lock()
	defer mu.Unlock()
	logger = l
}

// Interface for interacting with objects.
// If an object does not self describe via this interface
// than a reflection based implemenation will be used.
type SelfDescriber interface {
	//A description the object
	Desc() string

	HasChainMethod(name string) bool
	CallChainMethod(name string, args ...interface{}) (interface{}, error)

	HasProperty(name string) bool
	Property(name string) interface{}
	SetProperty(name string, args ...interface{}) (interface{}, error)
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

func errorf(p Position, fmtStr string, args ...interface{}) error {
	lineStr := fmt.Sprintf("line %d char %d: %s", p.Line(), p.Char(), fmtStr)
	return fmt.Errorf(lineStr, args...)
}

func wrapError(p Position, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("line %d char %d: %s", p.Line(), p.Char(), err.Error())
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
		err := evalUnary(node, node.Operator, scope, stck)
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
	case *DeclarationNode:
		err = eval(node.Left, scope, stck)
		if err != nil {
			return
		}
		err = eval(node.Right, scope, stck)
		if err != nil {
			return
		}
		err = evalDeclaration(scope, stck)
		if err != nil {
			return
		}
	case *ChainNode:
		err = eval(node.Left, scope, stck)
		if err != nil {
			return
		}
		err = eval(node.Right, scope, stck)
		if err != nil {
			return
		}
		err = evalChain(node, scope, stck)
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

func evalUnary(p Position, op tokenType, scope *Scope, stck *stack) error {
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
		switch num := v.(type) {
		case float64:
			stck.Push(-1 * num)
		case int64:
			stck.Push(-1 * num)
		case time.Duration:
			stck.Push(-1 * num)
		default:
			return errorf(p, "invalid arugument to '-' %v", v)
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
			return errorf(p, "invalid arugument to '!' %v", v)
		}
	}
	return nil
}

func evalDeclaration(scope *Scope, stck *stack) error {
	r := stck.Pop()
	l := stck.Pop()
	i := l.(*IdentifierNode)
	scope.Set(i.Ident, r)
	return nil
}

func evalChain(p Position, scope *Scope, stck *stack) error {
	r := stck.Pop()
	l := stck.Pop()
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
			var err error
			describer, err = NewReflectionDescriber(l)
			if err != nil {
				return wrapError(p, err)
			}
		}
		if describer.HasProperty(name) {
			stck.Push(describer.Property(name))
		} else {
			return errorf(p, "object %T has no property %s", l, name)
		}
	default:
		return errorf(p, "invalid right operand of type %T to '.' operator", r)
	}
	return nil
}

func evalFunc(f *FunctionNode, scope *Scope, stck *stack, args []interface{}) error {
	rec := func(obj interface{}, errp *error) {
		e := recover()
		if e != nil {
			*errp = fmt.Errorf("line %d char%d: error calling func %q on obj %T: %v", f.Line(), f.Char(), f.Func, obj, e)
			if strings.Contains((*errp).Error(), "*tick.ReferenceNode") && strings.Contains((*errp).Error(), "type string") {
				*errp = fmt.Errorf("line %d char%d: cannot assign *tick.ReferenceNode to type string, did you use double quotes instead of single quotes?", f.Line(), f.Char())
			}

		}
	}
	fnc := unboundFunc(func(obj interface{}) (_ interface{}, err error) {
		//Setup recover method if there is a panic during the method call
		defer rec(obj, &err)

		if f.Type == globalFunc {
			if obj != nil {
				return nil, fmt.Errorf("line %d char%d: calling global function on object %T", f.Line(), f.Char(), obj)
			}
			// Object is nil, check for func in scope
			fnc, _ := scope.Get(f.Func)
			if fnc == nil {
				return nil, fmt.Errorf("line %d char%d: no global function %q defined", f.Line(), f.Char(), f.Func)
			}
			method := reflect.ValueOf(fnc)
			o, err := callMethodReflection(method, args)
			return o, wrapError(f, err)
		}

		// Get SelfDescriber
		name := f.Func
		var describer SelfDescriber
		if d, ok := obj.(SelfDescriber); ok {
			describer = d
		} else {
			var err error
			describer, err = NewReflectionDescriber(obj)
			if err != nil {
				return nil, wrapError(f, err)
			}
		}

		// Call correct type of function
		switch f.Type {
		case chainFunc:
			if describer.HasChainMethod(name) {
				o, err := describer.CallChainMethod(name, args...)
				return o, wrapError(f, err)
			}
			if describer.HasProperty(name) {
				return nil, errorf(f, "no chaining method %q on %T, but property does exist. Use '.' operator instead: 'node.%s(..)'.", name, obj, name)
			}
			if dm := scope.DynamicMethod(name); dm != nil {
				return nil, errorf(f, "no chaining method %q on %T, but dynamic method does exist. Use '@' operator instead: 'node@%s(..)'.", name, obj, name)
			}
		case propertyFunc:
			if describer.HasProperty(name) {
				o, err := describer.SetProperty(name, args...)
				return o, wrapError(f, err)
			}
			if describer.HasChainMethod(name) {
				getLogger().Printf("W! DEPRECATED Syntax line %d char %d: found  use of '.' as chaining method. Please adopt new syntax 'node|%s(..)'.", f.Line(), f.Char(), name)
				o, err := describer.CallChainMethod(name, args...)
				return o, wrapError(f, err)
			}
			// Uncomment for 0.13 release, to finish deprecation of old syntax
			//if describer.HasChainMethod(name) {
			//	return nil, errorf(f, "no property method %q on %T, but chaining method does exist. Use '|' operator instead: 'node|%s(..)'.", name, obj, name)
			//}
			if dm := scope.DynamicMethod(name); dm != nil {
				// Uncomment for 0.13 release, to finish deprecation of old syntax
				//return nil, errorf(f, "no property method %q on %T, but dynamic method does exist. Use '@' operator instead: 'node@%s(..)'.", name, obj, name)
				getLogger().Printf("W! DEPRECATED Syntax line %d char %d: found use of '.' as dynamic method. Please adopt new syntax 'node@%s(...)'.", f.Line(), f.Char(), name)
				ret, err := dm(obj, args...)
				if err != nil {
					return nil, err
				}
				return ret, nil
			}
		case dynamicFunc:
			// Check for dynamic method.
			if dm := scope.DynamicMethod(name); dm != nil {
				ret, err := dm(obj, args...)
				if err != nil {
					return nil, err
				}
				return ret, nil
			}
			if describer.HasProperty(name) {
				return nil, errorf(f, "no dynamic method %q on %T, but property does exist. Use '.' operator instead: 'node.%s(..)'.", name, obj, name)
			}
			if describer.HasChainMethod(name) {
				return nil, errorf(f, "no dynamic method %q on %T, but chaining method does exist. Use '|' operator instead: 'node|%s(..)'.", name, obj, name)
			}
		default:
			return nil, errorf(f, "unknown function type %v on function %T.%s", f.Type, obj, name)
		}

		// Ran out of options...
		return nil, errorf(f, "no method or property %q on %T", name, obj)
	})
	stck.Push(fnc)
	return nil
}

// Wraps any object as a SelfDescriber using reflection.
type ReflectionDescriber struct {
	obj interface{}
	// Set of chain methods
	chainMethods map[string]reflect.Value
	// Set of methods that modify properties
	propertyMethods map[string]reflect.Value
	// Set of fields on obj that can be set
	properties map[string]reflect.Value
}

func NewReflectionDescriber(obj interface{}) (*ReflectionDescriber, error) {
	r := &ReflectionDescriber{
		obj: obj,
	}
	rv := reflect.ValueOf(r.obj)
	if !rv.IsValid() && rv.Kind() != reflect.Struct {
		return nil, fmt.Errorf("object is invalid %v of type %T", obj, obj)
	}
	rStructType := reflect.Indirect(rv).Type()
	rRecvType := reflect.TypeOf(r.obj)
	// Get all methods
	r.chainMethods = make(map[string]reflect.Value, rRecvType.NumMethod())
	for i := 0; i < rRecvType.NumMethod(); i++ {
		method := rRecvType.Method(i)
		if !rv.MethodByName(method.Name).IsValid() {
			return nil, fmt.Errorf("invalid method %s on type %T", method.Name, r.obj)
		}
		r.chainMethods[method.Name] = rv.MethodByName(method.Name)
	}

	// Get all properties
	var err error
	r.properties, r.propertyMethods, err = getProperties(r.Desc(), rv, rStructType, r.chainMethods)
	if err != nil {
		return nil, err
	}
	return r, nil
}

// Get properties from a struct and populate properties and propertyMethods maps, while removing
// and property methods from chainMethods.
// Recurses up anonymous fields.
func getProperties(desc string, rv reflect.Value, rStructType reflect.Type, chainMethods map[string]reflect.Value) (
	map[string]reflect.Value,
	map[string]reflect.Value,
	error) {
	properties := make(map[string]reflect.Value, rStructType.NumField())
	propertyMethods := make(map[string]reflect.Value)
	for i := 0; i < rStructType.NumField(); i++ {
		property := rStructType.Field(i)
		if property.Anonymous {
			// Recursively get properties from anon fields
			anonValue := reflect.Indirect(rv).Field(i)
			anonType := reflect.Indirect(anonValue).Type()
			props, propMethods, err := getProperties(desc, anonValue, anonType, chainMethods)
			if err != nil {
				return nil, nil, err
			}
			// Update local maps
			for k, v := range props {
				if _, ok := properties[k]; !ok {
					properties[k] = v
				}
			}
			for k, v := range propMethods {
				if _, ok := propertyMethods[k]; !ok {
					propertyMethods[k] = v
				}
			}
			continue
		}
		methodName := property.Tag.Get("tick")
		if methodName != "" {
			// Property is set via a property method.
			method := rv.MethodByName(methodName)
			if method.IsValid() {
				propertyMethods[methodName] = method
				// Remove property method from chainMethods.
				delete(chainMethods, methodName)
			} else {
				return nil, nil, fmt.Errorf("referenced method %s for type %s is invalid", methodName, desc)
			}
		} else {
			// Property is set directly via reflection.
			field := reflect.Indirect(rv).FieldByName(property.Name)
			if field.IsValid() && field.CanSet() {
				properties[property.Name] = field
			}
		}
	}
	return properties, propertyMethods, nil
}

func (r *ReflectionDescriber) Desc() string {
	return fmt.Sprintf("%T", r.obj)
}

// Using reflection check if the object has the method or field.
// A field is a valid method because we can set it via reflection too.
func (r *ReflectionDescriber) HasChainMethod(name string) bool {
	name = capilatizeFirst(name)
	_, ok := r.chainMethods[name]
	return ok
}

func (r *ReflectionDescriber) CallChainMethod(name string, args ...interface{}) (interface{}, error) {
	// Check for a method and call it
	name = capilatizeFirst(name)
	if method, ok := r.chainMethods[name]; ok {
		return callMethodReflection(method, args)
	}
	return nil, fmt.Errorf("unknown method %s on %T", name, r.obj)
}

// Using reflection check if the object has a field with the property name.
func (r *ReflectionDescriber) HasProperty(name string) bool {
	name = capilatizeFirst(name)
	_, ok := r.propertyMethods[name]
	if ok {
		return ok
	}
	_, ok = r.properties[name]
	return ok
}

func (r *ReflectionDescriber) Property(name string) interface{} {
	// Properties set by property methods cannot be read
	name = capilatizeFirst(name)
	property := r.properties[name]
	return property.Interface()
}

func (r *ReflectionDescriber) SetProperty(name string, values ...interface{}) (interface{}, error) {
	name = capilatizeFirst(name)
	propertyMethod, ok := r.propertyMethods[name]
	if ok {
		return callMethodReflection(propertyMethod, values)
	} else {
		if len(values) == 1 {
			property, ok := r.properties[name]
			if ok {
				v := reflect.ValueOf(values[0])
				property.Set(v)
				return r.obj, nil
			}
		} else {
			return nil, fmt.Errorf("too many arguments to set property %s on %T", name, r.obj)
		}
	}
	return nil, fmt.Errorf("no property %s on %T", name, r.obj)
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
	}
	return nil, fmt.Errorf("function must return a single value or (interface{}, error)")
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
		return valueToLiteralNode(node.position, v)
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
func valueToLiteralNode(p position, v interface{}) Node {
	switch value := v.(type) {
	case bool:
		return &BoolNode{
			position: p,
			Bool:     value,
		}
	case int64:
		return &NumberNode{
			position: p,
			IsInt:    true,
			Int64:    value,
		}
	case float64:
		return &NumberNode{
			position: p,
			IsFloat:  true,
			Float64:  value,
		}
	case time.Duration:
		return &DurationNode{
			position: p,
			Dur:      value,
		}
	case string:
		return &StringNode{
			position: p,
			Literal:  value,
		}
	case *regexp.Regexp:
		return &RegexNode{
			position: p,
			Regex:    value,
		}
	default:
		panic(errorf(p, "unsupported literal type %T", v))
	}
}
