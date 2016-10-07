package kapacitor

import (
	"fmt"
	"time"

	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/tick/stateful"
)

// EvalPredicate - Evaluate a given expression as a boolean predicate against a set of fields and tags
func EvalPredicate(se stateful.Expression, scopePool stateful.ScopePool, now time.Time, fields models.Fields, tags models.Tags) (bool, error) {
	vars := scopePool.Get()
	defer scopePool.Put(vars)
	err := fillScope(vars, scopePool.ReferenceVariables(), now, fields, tags)
	if err != nil {
		return false, err
	}

	b, err := se.EvalBool(vars)
	if err != nil {
		return false, err
	}
	return b, nil
}

// fillScope - given a scope and reference variables, we fill the exact variables from the now, fields and tags.
func fillScope(vars *stateful.Scope, referenceVariables []string, now time.Time, fields models.Fields, tags models.Tags) error {
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
				return fmt.Errorf("no field or tag exists for %s", refVariableName)
			}
		}
	}

	return nil
}
