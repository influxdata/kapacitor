package tick

import (
	"fmt"
	"strings"
)

type DynamicMethod func(self interface{}, args ...interface{}) (interface{}, error)

// Contains a set of variables references and their values.
type Scope struct {
	variables map[string]interface{}

	dynamicMethods map[string]DynamicMethod
}

//Initialize a new Scope object.
func NewScope() *Scope {
	return &Scope{
		variables:      make(map[string]interface{}),
		dynamicMethods: make(map[string]DynamicMethod),
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
	var possible []string
	for k := range s.variables {
		possible = append(possible, k)
	}
	return nil, fmt.Errorf("name %q is undefined. Names in scope: %s", name, strings.Join(possible, ","))
}

func (s *Scope) SetDynamicMethod(name string, m DynamicMethod) {
	s.dynamicMethods[name] = m
}

func (s *Scope) DynamicMethod(name string) DynamicMethod {
	return s.dynamicMethods[name]
}
