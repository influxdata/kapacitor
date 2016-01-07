package kapacitor

import (
	"fmt"

	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/tick"
)

// Evaluate a given expression as a boolean predicate against a set of fields and tags
func EvalPredicate(se *tick.StatefulExpr, fields models.Fields, tags map[string]string) (bool, error) {
	vars, err := mergeFieldsAndTags(fields, tags)
	if err != nil {
		return false, err
	}
	b, err := se.EvalBool(vars)
	if err != nil {
		return false, err
	}
	return b, nil
}

func mergeFieldsAndTags(fields models.Fields, tags map[string]string) (*tick.Scope, error) {
	scope := tick.NewScope()
	for k, v := range fields {
		if _, ok := tags[k]; ok {
			return nil, fmt.Errorf("cannot have field and tags with same name %q", k)
		}
		scope.Set(k, v)
	}
	for k, v := range tags {
		scope.Set(k, v)
	}
	return scope, nil
}
