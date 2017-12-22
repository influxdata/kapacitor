package kapacitor

import (
	"fmt"

	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/tick/ast"
	"github.com/influxdata/kapacitor/tick/stateful"
)

// EvalPredicate - Evaluate a given expression as a boolean predicate against a set of fields and tags
func EvalPredicate(se stateful.Expression, scopePool stateful.ScopePool, p edge.FieldsTagsTimeGetter) (bool, error) {
	vars := scopePool.Get()
	defer scopePool.Put(vars)
	err := fillScope(vars, scopePool.ReferenceVariables(), p)
	if err != nil {
		return false, err
	}

	// for function signature check
	if _, err := se.Type(vars); err != nil {
		return false, err
	}

	return se.EvalBool(vars)
}

// fillScope - given a scope and reference variables, we fill the exact variables from the now, fields and tags.
func fillScope(vars *stateful.Scope, referenceVariables []string, p edge.FieldsTagsTimeGetter) error {
	now := p.Time()
	fields := p.Fields()
	tags := p.Tags()
	for _, refVariableName := range referenceVariables {
		if refVariableName == "time" {
			vars.Set("time", now.Local())
			continue
		}

		// Support the error with tags/fields collision
		var fieldValue interface{}
		var isFieldExists bool
		var tagValue interface{}
		var isTagExists bool

		if fieldValue, isFieldExists = fields[refVariableName]; isFieldExists {
			vars.Set(refVariableName, fieldValue)
		}

		if tagValue, isTagExists = tags[refVariableName]; isTagExists {
			if isFieldExists {
				return fmt.Errorf("cannot have field and tags with same name %q", refVariableName)
			}
			vars.Set(refVariableName, tagValue)
		}
		if !isFieldExists && !isTagExists {
			if !vars.Has(refVariableName) {
				vars.Set(refVariableName, ast.MissingValue)
			}

		}
	}

	return nil
}
