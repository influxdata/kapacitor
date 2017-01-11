package tick

import (
	"errors"
	"fmt"
	goast "go/ast"
	"log"
	"os"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/influxdata/kapacitor/tick/ast"
	"github.com/influxdata/kapacitor/tick/stateful"
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

type unboundFunc func(obj interface{}) (interface{}, error)

type Var struct {
	Value       interface{}
	Type        ast.ValueType
	Description string
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

// PartialDescriber can provide a description
// of its chain methods that hide embedded property methods.
type PartialDescriber interface {
	ChainMethods() map[string]reflect.Value
}

// Parse and evaluate a given script for the scope.
// Returns a set of default vars.
// If a set of predefined vars is provided, they may effect the default var values.
func Evaluate(script string, scope *stateful.Scope, predefinedVars map[string]Var, ignoreMissingVars bool) (_ map[string]Var, err error) {
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

	root, err := ast.Parse(script)
	if err != nil {
		return nil, err
	}

	// Use a stack machine to evaluate the AST
	stck := &stack{}
	// Collect any defined defaultVars
	defaultVars := make(map[string]Var)
	err = eval(root, scope, stck, predefinedVars, defaultVars, ignoreMissingVars)
	if err != nil {
		return nil, err
	}
	return defaultVars, nil
}

func errorf(p ast.Position, fmtStr string, args ...interface{}) error {
	lineStr := fmt.Sprintf("line %d char %d: %s", p.Line(), p.Char(), fmtStr)
	return fmt.Errorf(lineStr, args...)
}

func wrapError(p ast.Position, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("line %d char %d: %s", p.Line(), p.Char(), err.Error())
}

// Evaluate a node using a stack machine in a given scope
func eval(n ast.Node, scope *stateful.Scope, stck *stack, predefinedVars, defaultVars map[string]Var, ignoreMissingVars bool) (err error) {
	switch node := n.(type) {
	case *ast.BoolNode:
		stck.Push(node.Bool)
	case *ast.NumberNode:
		if node.IsInt {
			stck.Push(node.Int64)
		} else {
			stck.Push(node.Float64)
		}
	case *ast.DurationNode:
		stck.Push(node.Dur)
	case *ast.StringNode:
		stck.Push(node.Literal)
	case *ast.RegexNode:
		stck.Push(node.Regex)
	case *ast.UnaryNode:
		err = eval(node.Node, scope, stck, predefinedVars, defaultVars, ignoreMissingVars)
		if err != nil {
			return
		}
		err = evalUnary(node, node.Operator, scope, stck)
		if err != nil {
			return
		}
	case *ast.BinaryNode:
		// Switch over to using the stateful expressions for evaluating a BinaryNode
		n, err := resolveIdents(node, scope)
		if err != nil {
			return err
		}
		expr, err := stateful.NewExpression(n)
		if err != nil {
			return err
		}
		value, err := expr.Eval(stateful.NewScope())
		if err != nil {
			return err
		}
		stck.Push(value)
	case *ast.LambdaNode:
		node.Expression, err = resolveIdents(node.Expression, scope)
		if err != nil {
			return
		}
		stck.Push(node)
	case *ast.ListNode:
		nodes := make([]interface{}, len(node.Nodes))
		for i, n := range node.Nodes {
			err = eval(n, scope, stck, predefinedVars, defaultVars, ignoreMissingVars)
			if err != nil {
				return
			}
			a := stck.Pop()
			switch typed := a.(type) {
			case *ast.IdentifierNode:
				// Resolve identifier
				a, err = scope.Get(typed.Ident)
				if err != nil {
					return err
				}
			}

			nodes[i] = a
		}
		stck.Push(nodes)
	case *ast.TypeDeclarationNode:
		err = evalTypeDeclaration(node, scope, predefinedVars, defaultVars, ignoreMissingVars)
		if err != nil {
			return
		}
	case *ast.DeclarationNode:
		err = eval(node.Right, scope, stck, predefinedVars, defaultVars, ignoreMissingVars)
		if err != nil {
			return
		}
		err = evalDeclaration(node, scope, stck, predefinedVars, defaultVars)
		if err != nil {
			return
		}
	case *ast.ChainNode:
		err = eval(node.Left, scope, stck, predefinedVars, defaultVars, ignoreMissingVars)
		if err != nil {
			return
		}
		err = eval(node.Right, scope, stck, predefinedVars, defaultVars, ignoreMissingVars)
		if err != nil {
			return
		}
		err = evalChain(node, scope, stck)
		if err != nil {
			return
		}
	case *ast.FunctionNode:
		args := make([]interface{}, len(node.Args))
		for i, arg := range node.Args {
			err = eval(arg, scope, stck, predefinedVars, defaultVars, ignoreMissingVars)
			if err != nil {
				return
			}
			a := stck.Pop()
			switch typed := a.(type) {
			case *ast.IdentifierNode:
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
	case *ast.ProgramNode:
		for _, n := range node.Nodes {
			err = eval(n, scope, stck, predefinedVars, defaultVars, ignoreMissingVars)
			if err != nil {
				return
			}
			// Pop unused result
			if stck.Len() > 0 {
				ret := stck.Pop()
				if f, ok := ret.(unboundFunc); ok {
					// Call global function
					_, err := f(nil)
					if err != nil {
						return err
					}
				}
			}
		}
	default:
		stck.Push(node)
	}
	return nil
}

func evalUnary(p ast.Position, op ast.TokenType, scope *stateful.Scope, stck *stack) error {
	v := stck.Pop()
	switch op {
	case ast.TokenMinus:
		if ident, ok := v.(*ast.IdentifierNode); ok {
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
	case ast.TokenNot:
		if ident, ok := v.(*ast.IdentifierNode); ok {
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

func evalTypeDeclaration(node *ast.TypeDeclarationNode, scope *stateful.Scope, predefinedVars, defaultVars map[string]Var, ignoreMissingVars bool) error {
	var actualType ast.ValueType
	switch node.Type.Ident {
	case "int":
		actualType = ast.TInt
	case "float":
		actualType = ast.TFloat
	case "bool":
		actualType = ast.TBool
	case "string":
		actualType = ast.TString
	case "regex":
		actualType = ast.TRegex
	case "duration":
		actualType = ast.TDuration
	case "lambda":
		actualType = ast.TLambda
	case "list":
		actualType = ast.TList
	case "star":
		actualType = ast.TStar
	default:
		return fmt.Errorf("invalid var type %q", node.Type.Ident)
	}
	name := node.Node.Ident
	desc := ""
	if node.Comment != nil {
		desc = node.Comment.CommentString()
	}
	defaultVars[name] = Var{
		Type:        actualType,
		Value:       nil,
		Description: desc,
	}

	if predefinedValue, ok := predefinedVars[name]; ok {
		if predefinedValue.Type != actualType {
			return fmt.Errorf("invalid type supplied for %s, got %v exp %v", name, predefinedValue.Type, actualType)
		}
		v, err := convertVarToValue(Var{Value: predefinedValue.Value, Type: actualType})
		if err != nil {
			return err
		}
		scope.Set(name, v)
	} else if ignoreMissingVars {
		// Set zero value on scope, so execution can continue
		scope.Set(name, ast.ZeroValue(actualType))
	} else {
		return fmt.Errorf("missing value for var %q.", name)
	}

	return nil
}

func convertVarToValue(v Var) (interface{}, error) {
	value := v.Value
	if v.Type == ast.TList {
		values, ok := value.([]Var)
		if !ok {
			return nil, fmt.Errorf("var has type list but value is type %T", value)
		}

		list := make([]interface{}, len(values))
		for i := range values {
			list[i] = values[i].Value
		}
		value = list
	}
	return value, nil
}

func convertValueToVar(value interface{}, typ ast.ValueType, desc string) (Var, error) {
	varValue := value
	if typ == ast.TList {
		values, ok := value.([]interface{})
		if !ok {
			return Var{}, fmt.Errorf("var has type list but value is type %T", value)
		}

		list := make([]Var, len(values))
		for i := range values {
			typ := ast.TypeOf(values[i])
			list[i] = Var{
				Type:  typ,
				Value: values[i],
			}
		}
		varValue = list
	}
	return Var{
		Type:        typ,
		Value:       varValue,
		Description: desc,
	}, nil
}

func evalDeclaration(node *ast.DeclarationNode, scope *stateful.Scope, stck *stack, predefinedVars, defaultVars map[string]Var) error {
	name := node.Left.Ident
	if v, _ := scope.Get(name); v != nil {
		return fmt.Errorf("attempted to redefine %s, vars are immutable", name)
	}
	value := stck.Pop()
	if i, ok := value.(*ast.IdentifierNode); ok {
		// Resolve identifier
		v, err := scope.Get(i.Ident)
		if err != nil {
			return err
		}
		value = v
	}
	actualType := ast.TypeOf(value)
	// Populate set of default vars
	if actualType != ast.InvalidType {
		desc := ""
		if node.Comment != nil {
			desc = node.Comment.CommentString()
		}

		v, err := convertValueToVar(value, actualType, desc)
		if err != nil {
			return err
		}
		defaultVars[name] = v
	}
	// Populate scope, first check for predefined var
	if predefinedValue, ok := predefinedVars[name]; ok {
		if predefinedValue.Type != actualType {
			return fmt.Errorf("invalid type supplied for %s, got %v exp %v", name, predefinedValue.Type, actualType)
		}
		v, err := convertVarToValue(Var{Value: predefinedValue.Value, Type: actualType})
		if err != nil {
			return err
		}
		value = v
	}
	scope.Set(name, value)
	return nil
}

func evalChain(p ast.Position, scope *stateful.Scope, stck *stack) error {
	r := stck.Pop()
	l := stck.Pop()
	// Resolve identifier
	if left, ok := l.(*ast.IdentifierNode); ok {
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
	case *ast.IdentifierNode:
		name := right.Ident

		//Lookup field by name of left object
		var describer SelfDescriber
		if d, ok := l.(SelfDescriber); ok {
			describer = d
		} else {
			var err error
			var extraChainMethods map[string]reflect.Value
			if pd, ok := l.(PartialDescriber); ok {
				extraChainMethods = pd.ChainMethods()
			}
			describer, err = NewReflectionDescriber(l, extraChainMethods)
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

func evalFunc(f *ast.FunctionNode, scope *stateful.Scope, stck *stack, args []interface{}) error {
	// If the first and only arg is a list use it as the list of args
	if len(args) == 1 {
		if a, ok := args[0].([]interface{}); ok {
			args = a
		}
	}
	rec := func(obj interface{}, errp *error) {
		e := recover()
		if e != nil {
			*errp = fmt.Errorf("line %d char %d: error calling func %q on obj %T: %v", f.Line(), f.Char(), f.Func, obj, e)
			if strings.Contains((*errp).Error(), "*ast.ReferenceNode") && strings.Contains((*errp).Error(), "type string") {
				*errp = fmt.Errorf("line %d char %d: cannot assign *ast.ReferenceNode to type string, did you use double quotes instead of single quotes?", f.Line(), f.Char())
			}

		}
	}
	fnc := unboundFunc(func(obj interface{}) (_ interface{}, err error) {
		//Setup recover method if there is a panic during the method call
		defer rec(obj, &err)

		if f.Type == ast.GlobalFunc {
			if obj != nil {
				return nil, fmt.Errorf("line %d char %d: calling global function on object %T", f.Line(), f.Char(), obj)
			}
			// Object is nil, check for func in scope
			fnc, _ := scope.Get(f.Func)
			if fnc == nil {
				return nil, fmt.Errorf("line %d char %d: no global function %q defined", f.Line(), f.Char(), f.Func)
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
			var extraChainMethods map[string]reflect.Value
			if pd, ok := obj.(PartialDescriber); ok {
				extraChainMethods = pd.ChainMethods()
			}
			describer, err = NewReflectionDescriber(obj, extraChainMethods)
			if err != nil {
				return nil, wrapError(f, err)
			}
		}

		// Call correct type of function
		switch f.Type {
		case ast.ChainFunc:
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
		case ast.PropertyFunc:
			if describer.HasProperty(name) {
				o, err := describer.SetProperty(name, args...)
				return o, wrapError(f, err)
			}
			if describer.HasChainMethod(name) {
				return nil, errorf(f, "no property method %q on %T, but chaining method does exist. Use '|' operator instead: 'node|%s(..)'.", name, obj, name)
			}
			if dm := scope.DynamicMethod(name); dm != nil {
				return nil, errorf(f, "no property method %q on %T, but dynamic method does exist. Use '@' operator instead: 'node@%s(..)'.", name, obj, name)
			}
		case ast.DynamicFunc:
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
//
// Uses tags on fields to determine if a method is really a PropertyMethod
// Can disambiguate property fields and chain methods of the same name but
// from different composed anonymous fields.
// Cannot disambiguate property methods and chain methods of the same name.
// See NewReflectionDescriber for providing explicit chain methods in this case.
//
// Example:
//     type MyType struct {
//         UseX `tick:"X"`
//     }
//     func (m *MyType) X() *MyType{
//         m.UseX = true
//         return m
//     }
//
// UseX will be ignored as a property and the method X will become a property method.
//
//
// Expects that all callable methods are pointer receiver methods.
type ReflectionDescriber struct {
	obj interface{}
	// Set of chain methods
	chainMethods map[string]reflect.Value
	// Set of methods that modify properties
	propertyMethods map[string]reflect.Value
	// Set of fields on obj that can be set
	properties map[string]reflect.Value
}

// Create a NewReflectionDescriber from an object.
// The object must be a pointer type.
// Use the chainMethods parameter to provide a set of explicit methods
// that should be considered chain methods even if an embedded type declares them as property methods
//
// Example:
//     type MyType struct {
//         UseX `tick:"X"`
//     }
//     func (m *MyType) X() *MyType{
//         m.UseX = true
//         return m
//     }
//
//     type AnotherType struct {
//         MyType
//     }
//     func (a *AnotherType) X() *YetAnotherType {
//         // do chain method work here...
//     }
//
//     // Now create NewReflectionDescriber with X as a chain method and property method
//     at := new(AnotherType)
//     rd := NewReflectionDescriber(at, map[string]reflect.Value{
//         "X": reflect.ValueOf(at.X),
//     })
//     rd.HasProperty("x") // true
//     rd.HasChainMethod("x") // true
//
func NewReflectionDescriber(obj interface{}, chainMethods map[string]reflect.Value) (*ReflectionDescriber, error) {
	r := &ReflectionDescriber{
		obj: obj,
	}
	rv := reflect.ValueOf(r.obj)
	if !rv.IsValid() && rv.Kind() != reflect.Struct {
		return nil, fmt.Errorf("object is invalid %v of type %T", obj, obj)
	}

	// Get all properties
	var err error
	r.properties, r.propertyMethods, err = getProperties(r.Desc(), rv)
	if err != nil {
		return nil, err
	}

	// Get all methods
	r.chainMethods, err = getChainMethods(r.Desc(), rv, r.propertyMethods)
	if err != nil {
		return nil, err
	}
	for k, v := range chainMethods {
		r.chainMethods[k] = v
	}

	return r, nil
}

// Get properties from a struct and populate properties and propertyMethods maps
// Recurses up anonymous fields.
func getProperties(
	desc string,
	rv reflect.Value,
) (
	map[string]reflect.Value,
	map[string]reflect.Value,
	error,
) {
	if rv.Kind() != reflect.Ptr {
		return nil, nil, errors.New("cannot get properties of non pointer value")
	}
	element := rv.Elem()
	if !element.IsValid() {
		return nil, nil, errors.New("cannot get properties of nil pointer")
	}
	rStructType := element.Type()
	if rStructType.Kind() != reflect.Struct {
		return nil, nil, errors.New("cannot get properties of non struct")
	}
	properties := make(map[string]reflect.Value, rStructType.NumField())
	propertyMethods := make(map[string]reflect.Value)
	for i := 0; i < rStructType.NumField(); i++ {
		property := rStructType.Field(i)
		if property.Anonymous {
			// Recursively get properties from anon fields
			anonValue := reflect.Indirect(rv).Field(i)
			if anonValue.Kind() != reflect.Ptr && anonValue.CanAddr() {
				anonValue = anonValue.Addr()
			}
			if anonValue.Kind() == reflect.Ptr && anonValue.IsNil() {
				// Skip nil fields
				continue
			}
			props, propMethods, err := getProperties(fmt.Sprintf("%s.%s", desc, property.Name), anonValue)
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
			if !method.IsValid() && rv.CanAddr() {
				method = rv.Addr().MethodByName(methodName)
			}
			if method.IsValid() {
				propertyMethods[methodName] = method
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

func getChainMethods(desc string, rv reflect.Value, propertyMethods map[string]reflect.Value) (map[string]reflect.Value, error) {
	if rv.Kind() != reflect.Ptr {
		return nil, errors.New("cannot get chain methods of non pointer")
	}
	element := rv.Elem()
	if !element.IsValid() {
		return nil, errors.New("cannot get chain methods of nil pointer")
	}
	// Find all methods on value
	rRecvType := rv.Type()
	chainMethods := make(map[string]reflect.Value, rRecvType.NumMethod())
	for i := 0; i < rRecvType.NumMethod(); i++ {
		method := rRecvType.Method(i)
		if !goast.IsExported(method.Name) {
			continue
		}
		if !rv.MethodByName(method.Name).IsValid() {
			return nil, fmt.Errorf("invalid method %s on type %s", method.Name, desc)
		}
		if _, exists := propertyMethods[method.Name]; !exists {
			chainMethods[method.Name] = rv.MethodByName(method.Name)
		}
	}

	// Find all methods from anonymous fields.
	rStructType := element.Type()
	for i := 0; i < rStructType.NumField(); i++ {
		field := rStructType.Field(i)
		if field.Anonymous {
			anonValue := element.Field(i)
			if anonValue.Kind() != reflect.Ptr && anonValue.CanAddr() {
				anonValue = anonValue.Addr()
			}
			anonChainMethods, err := getChainMethods(fmt.Sprintf("%s.%s", desc, field.Name), anonValue, propertyMethods)
			if err != nil {
				return nil, err
			}
			for k, v := range anonChainMethods {
				if _, exists := chainMethods[k]; !exists {
					chainMethods[k] = v
				}
			}
		}
	}
	return chainMethods, nil
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
func resolveIdents(n ast.Node, scope *stateful.Scope) (_ ast.Node, err error) {
	switch node := n.(type) {
	case *ast.IdentifierNode:
		v, err := scope.Get(node.Ident)
		if err != nil {
			return nil, err
		}
		lit, err := ast.ValueToLiteralNode(node, v)
		if err != nil {
			return nil, err
		}
		return lit, nil
	case *ast.UnaryNode:
		node.Node, err = resolveIdents(node.Node, scope)
		if err != nil {
			return nil, err
		}
	case *ast.BinaryNode:
		node.Left, err = resolveIdents(node.Left, scope)
		if err != nil {
			return nil, err
		}
		node.Right, err = resolveIdents(node.Right, scope)
		if err != nil {
			return nil, err
		}
	case *ast.FunctionNode:
		for i, arg := range node.Args {
			node.Args[i], err = resolveIdents(arg, scope)
			if err != nil {
				return nil, err
			}
		}
	case *ast.ProgramNode:
		for i, n := range node.Nodes {
			node.Nodes[i], err = resolveIdents(n, scope)
			if err != nil {
				return nil, err
			}
		}
	}
	return n, nil
}
