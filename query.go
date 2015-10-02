package kapacitor

import (
	"fmt"
	"time"

	"github.com/influxdb/influxdb/influxql"
)

type Query struct {
	startTL *influxql.TimeLiteral
	stopTL  *influxql.TimeLiteral
	q       *influxql.SelectStatement
}

func NewQuery(q string) (*Query, error) {
	query := &Query{}
	// Parse and validate query
	stmt, err := influxql.ParseStatement(q)
	if err != nil {
		return nil, err
	}
	var ok bool
	query.q, ok = stmt.(*influxql.SelectStatement)
	if !ok {
		return nil, fmt.Errorf("query is not a select statement %q", q)
	}

	// Add in time condition nodes
	query.startTL = &influxql.TimeLiteral{}
	startExpr := &influxql.BinaryExpr{
		Op:  influxql.GT,
		LHS: &influxql.VarRef{Val: "time"},
		RHS: query.startTL,
	}

	query.stopTL = &influxql.TimeLiteral{}
	stopExpr := &influxql.BinaryExpr{
		Op:  influxql.LT,
		LHS: &influxql.VarRef{Val: "time"},
		RHS: query.stopTL,
	}

	if query.q.Condition != nil {
		query.q.Condition = &influxql.BinaryExpr{
			Op:  influxql.AND,
			LHS: query.q.Condition,
			RHS: &influxql.BinaryExpr{
				Op:  influxql.AND,
				LHS: startExpr,
				RHS: stopExpr,
			},
		}
	} else {
		query.q.Condition = &influxql.BinaryExpr{
			Op:  influxql.AND,
			LHS: startExpr,
			RHS: stopExpr,
		}
	}
	return query, nil
}

// Set the start time of the query
func (q *Query) Start(s time.Time) {
	q.startTL.Val = s
}

// Set the stop time of the query
func (q *Query) Stop(s time.Time) {
	q.stopTL.Val = s
}

// Set the dimensions on the query
func (q *Query) Dimensions(dims []interface{}) error {
	q.q.Dimensions = q.q.Dimensions[:0]
	// Add in dimensions
	hasTime := false
	for _, d := range dims {
		switch dim := d.(type) {
		case time.Duration:
			if hasTime {
				return fmt.Errorf("groupBy cannot have more than one time dimension")
			}
			// Add time dimension
			hasTime = true
			q.q.Dimensions = append(q.q.Dimensions,
				&influxql.Dimension{
					Expr: &influxql.Call{
						Name: "time",
						Args: []influxql.Expr{
							&influxql.DurationLiteral{
								Val: dim,
							},
						},
					},
				})
		case string:
			q.q.Dimensions = append(q.q.Dimensions,
				&influxql.Dimension{
					Expr: &influxql.VarRef{
						Val: dim,
					},
				})
		default:
			return fmt.Errorf("invalid dimension type:%T, must be string or time.Duration", d)
		}
	}

	if !hasTime {
		return fmt.Errorf("groupBy must have a time dimension.")
	}
	return nil
}

func (q *Query) String() string {
	return q.q.String()
}
