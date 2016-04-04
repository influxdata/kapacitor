package kapacitor

import (
	"fmt"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/kapacitor/tick"
)

type Query struct {
	startTL *influxql.TimeLiteral
	stopTL  *influxql.TimeLiteral
	stmt    *influxql.SelectStatement
}

func NewQuery(q string) (*Query, error) {
	query := &Query{}
	// Parse and validate query
	stmt, err := influxql.ParseStatement(q)
	if err != nil {
		return nil, err
	}
	var ok bool
	query.stmt, ok = stmt.(*influxql.SelectStatement)
	if !ok {
		return nil, fmt.Errorf("query is not a select statement %q", q)
	}

	// Add in time condition nodes
	query.startTL = &influxql.TimeLiteral{}
	startExpr := &influxql.BinaryExpr{
		Op:  influxql.GTE,
		LHS: &influxql.VarRef{Val: "time"},
		RHS: query.startTL,
	}

	query.stopTL = &influxql.TimeLiteral{}
	stopExpr := &influxql.BinaryExpr{
		Op:  influxql.LT,
		LHS: &influxql.VarRef{Val: "time"},
		RHS: query.stopTL,
	}

	if query.stmt.Condition != nil {
		query.stmt.Condition = &influxql.BinaryExpr{
			Op:  influxql.AND,
			LHS: query.stmt.Condition,
			RHS: &influxql.BinaryExpr{
				Op:  influxql.AND,
				LHS: startExpr,
				RHS: stopExpr,
			},
		}
	} else {
		query.stmt.Condition = &influxql.BinaryExpr{
			Op:  influxql.AND,
			LHS: startExpr,
			RHS: stopExpr,
		}
	}
	return query, nil
}

// Return the db rp pairs of the query
func (q *Query) DBRPs() ([]DBRP, error) {
	dbrps := make([]DBRP, len(q.stmt.Sources))
	for i, s := range q.stmt.Sources {
		m, ok := s.(*influxql.Measurement)
		if !ok {
			return nil, fmt.Errorf("unknown query source %T", s)
		}
		dbrps[i] = DBRP{
			Database:        m.Database,
			RetentionPolicy: m.RetentionPolicy,
		}
	}
	return dbrps, nil
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
	q.stmt.Dimensions = q.stmt.Dimensions[:0]
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
			q.stmt.Dimensions = append(q.stmt.Dimensions,
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
			q.stmt.Dimensions = append(q.stmt.Dimensions,
				&influxql.Dimension{
					Expr: &influxql.VarRef{
						Val: dim,
					},
				})
		case *tick.StarNode:
			q.stmt.Dimensions = append(q.stmt.Dimensions,
				&influxql.Dimension{
					Expr: &influxql.Wildcard{},
				})

		default:
			return fmt.Errorf("invalid dimension type:%T, must be string or time.Duration", d)
		}
	}

	return nil
}

func (q *Query) Fill(option influxql.FillOption, value interface{}) {
	q.stmt.Fill = option
	q.stmt.FillValue = value
}

func (q *Query) String() string {
	return q.stmt.String()
}
