package kapacitor

import (
	"fmt"

	"github.com/influxdb/kapacitor/expr"
	"github.com/influxdb/kapacitor/models"
)

func ExprFunc(field, e string) (TransFunc, error) {
	t, err := expr.Parse(e)
	if err != nil {
		return nil, err
	}
	if t.RType() != expr.ReturnNum {
		return nil, fmt.Errorf("expression does not evaluate to a number")
	}
	x := &expression{
		field: field,
		se:    &expr.StatefulExpr{Tree: t, Funcs: expr.Functions()},
	}
	return TransFunc(x.Eval), nil
}

type expression struct {
	field string
	se    *expr.StatefulExpr
}

func (x *expression) Eval(fields models.Fields) (models.Fields, error) {
	vars := make(expr.Vars)
	for k, v := range fields {
		if f, ok := v.(float64); ok {
			vars[k] = f
		} else {
			return nil, fmt.Errorf("field values must be float64")
		}
	}

	v, err := x.se.EvalNum(vars)
	if err != nil {
		return nil, err
	}
	fields[x.field] = v
	return fields, nil
}

// Evaluate a given expression as a boolean predicate against a set of fields and tags
func EvalPredicate(se *expr.StatefulExpr, fields models.Fields, tags map[string]string) (bool, error) {
	vars := make(expr.Vars)
	for k, v := range fields {
		if tags[k] != "" {
			return false, fmt.Errorf("cannot have field and tags with same name %q", k)
		}
		vars[k] = v
	}
	for k, v := range tags {
		vars[k] = v
	}
	b, err := se.EvalBool(vars)
	if err != nil {
		return false, err
	}
	return b, nil
}
