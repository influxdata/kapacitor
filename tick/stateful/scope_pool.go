package stateful

import (
	"sync"

	"github.com/influxdata/kapacitor/tick"
)

// ScopePool - pooling mechanism for tick.Scope
// The idea behind scope pool is to pool scopes and to put them only
// the needed variables for execution.
type ScopePool interface {
	Get() *tick.Scope
	Put(scope *tick.Scope)

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
			scope := tick.NewScope()
			for _, refVariable := range scopePool.referenceVariables {
				scope.Set(refVariable, nil)
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
func (s *scopePool) Get() *tick.Scope {
	return s.pool.Get().(*tick.Scope)
}

// Put - put used scope back to the pool
func (s *scopePool) Put(scope *tick.Scope) {
	s.pool.Put(scope)
}
