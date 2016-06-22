package stateful

import "sync"

// ScopePool - pooling mechanism for Scope
// The idea behind scope pool is to pool scopes and to put them only
// the needed variables for execution.
type ScopePool interface {
	Get() *Scope
	Put(scope *Scope)

	ReferenceVariables() []string
}

type scopePool struct {
	referenceVariables []string
	pool               sync.Pool
}

// NewScopePool - creates new ScopePool for the given Node
func NewScopePool(referenceVariables []string) ScopePool {
	scopePool := &scopePool{
		referenceVariables: referenceVariables,
	}

	scopePool.pool = sync.Pool{
		New: func() interface{} {
			scope := NewScope()
			for _, refVariable := range scopePool.referenceVariables {
				scope.Set(refVariable, empty)
			}

			return scope
		},
	}

	return scopePool
}

func (s *scopePool) ReferenceVariables() []string {
	return s.referenceVariables
}

// Get - returns a scope from a pool with the needed reference variables
// (with nil values/old values) in the scope
func (s *scopePool) Get() *Scope {
	return s.pool.Get().(*Scope)
}

// Put - put used scope back to the pool
func (s *scopePool) Put(scope *Scope) {
	scope.Reset()
	s.pool.Put(scope)
}
