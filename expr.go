package kapacitor

import (
	"fmt"
	"time"

	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/tick"
)

// Evaluate a given expression as a boolean predicate against a set of fields and tags
func EvalPredicate(se *tick.StatefulExpr, now time.Time, fields models.Fields, tags models.Tags) (bool, error) {
	vars, err := mergeFieldsAndTags(now, fields, tags)
	if err != nil {
		return false, err
	}
	b, err := se.EvalBool(vars)
	if err != nil {
		return false, err
	}
	return b, nil
}

func mergeFieldsAndTags(now time.Time, fields models.Fields, tags models.Tags) (*tick.Scope, error) {
	scope := tick.NewScope()
	scope.Set("time", now)
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
