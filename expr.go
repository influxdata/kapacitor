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
	if t.RType() != expr.ReturnNumber {
		return nil, fmt.Errorf("expression does not evaluate to a number")
	}
	x := &expression{
		field: field,
		t:     t,
	}
	return TransFunc(x.Eval), nil
}

type expression struct {
	field string
	t     *expr.Tree
}

func (x *expression) Eval(p *models.Point) (*models.Point, error) {
	vars := make(expr.Vars)
	for k, v := range p.Fields {
		if f, ok := v.(float64); ok {
			vars[k] = f
		} else {
			return nil, fmt.Errorf("field values must be float64")
		}
	}

	fields := make(map[string]interface{}, 1)
	v, err := x.t.EvalNumber(vars, nil)
	if err != nil {
		return nil, err
	}
	fields[x.field] = v
	np := models.NewPoint(
		p.Name,
		p.Group,
		p.Tags,
		fields,
		p.Time,
	)
	return np, nil
}
