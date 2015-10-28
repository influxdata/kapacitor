package tick

import (
	"fmt"
)

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

// Get returns the value of 'name'.
func (s *Scope) Get(name string) (interface{}, error) {
	if v, ok := s.variables[name]; ok {
		return v, nil
	}
	return nil, fmt.Errorf("name %q is undefined", name)
}
