package tick

// Contains a set of variables references and their values.
type Scope struct {
	variables map[string]interface{}
}

//Initialize a new Scope object.
func NewScope() *Scope {
	return &Scope{
		variables: make(map[string]interface{}),
	}
}

// Set defines a name -> value pairing in the scope.
func (s *Scope) Set(name string, value interface{}) {
	s.variables[name] = value
}

// Get returns the value of 'name' or nil if it does not exist.
func (s *Scope) Get(name string) interface{} {
	if v, ok := s.variables[name]; ok {
		return v
	}
	return nil
}

// Evaluate a given DSL script for a given scope.
func Evaluate(script string, scope *Scope) (err error) {

	ast, err := parse(script)
	if err != nil {
		return err
	}

	err = ast.Eval(scope)
	return
}
