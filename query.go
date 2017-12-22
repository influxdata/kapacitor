package kapacitor

import (
	"fmt"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/kapacitor/tick/ast"
	"github.com/pkg/errors"
)

type Query struct {
	startTL         *influxql.TimeLiteral
	stopTL          *influxql.TimeLiteral
	groupByTimeDL   *influxql.DurationLiteral
	groupByOffsetDL *influxql.DurationLiteral
	stmt            *influxql.SelectStatement
	alignGroup      bool
}

func NewQuery(queryString string) (*Query, error) {
	query := &Query{}
	// Parse and validate query
	q, err := influxql.ParseQuery(queryString)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse InfluxQL query")
	}
	if l := len(q.Statements); l != 1 {
		return nil, fmt.Errorf("query must be a single select statement, got %d statements", l)
	}
	var ok bool
	query.stmt, ok = q.Statements[0].(*influxql.SelectStatement)
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
func (q *Query) StartTime() time.Time {
	return q.startTL.Val
}

// Set the stop time of the query
func (q *Query) StopTime() time.Time {
	return q.stopTL.Val
}

// Set the start time of the query
func (q *Query) SetStartTime(s time.Time) {
	q.startTL.Val = s
	if q.alignGroup && q.groupByTimeDL != nil && q.groupByOffsetDL != nil {
		q.groupByOffsetDL.Val = s.Sub(time.Unix(0, 0)) % q.groupByTimeDL.Val
	}
}

// Set the stop time of the query
func (q *Query) SetStopTime(s time.Time) {
	q.stopTL.Val = s
}

// Deep clone this query
func (q *Query) Clone() (*Query, error) {
	n := &Query{
		stmt:       q.stmt.Clone(),
		alignGroup: q.alignGroup,
	}
	// Find the start/stop time literals
	var err error
	influxql.WalkFunc(n.stmt.Condition, func(qlNode influxql.Node) {
		if bn, ok := qlNode.(*influxql.BinaryExpr); ok {
			switch bn.Op {
			case influxql.GTE:
				if vf, ok := bn.LHS.(*influxql.VarRef); !ok || vf.Val != "time" {
					return
				}
				if tl, ok := bn.RHS.(*influxql.TimeLiteral); ok {
					// We have a "time" >= 'time literal'
					if n.startTL == nil {
						n.startTL = tl
					} else {
						err = errors.New("invalid query, found multiple start time conditions")
					}
				}
			case influxql.LT:
				if vf, ok := bn.LHS.(*influxql.VarRef); !ok || vf.Val != "time" {
					return
				}
				if tl, ok := bn.RHS.(*influxql.TimeLiteral); ok {
					// We have a "time" < 'time literal'
					if n.stopTL == nil {
						n.stopTL = tl
					} else {
						err = errors.New("invalid query, found multiple stop time conditions")
					}
				}
			}
		}
	})
	influxql.WalkFunc(n.stmt.Dimensions, func(qlNode influxql.Node) {
		if cn, ok := qlNode.(*influxql.Call); ok {
			if cn.Name == "time" {
				if dln, ok := cn.Args[0].(*influxql.DurationLiteral); ok {
					n.groupByTimeDL = &influxql.DurationLiteral{
						Val: dln.Val,
					}
				}
				if don, ok := cn.Args[1].(*influxql.DurationLiteral); ok {
					n.groupByOffsetDL = &influxql.DurationLiteral{
						Val: don.Val,
					}
				}
			}
		}
	})
	if n.startTL == nil {
		err = errors.New("invalid query, missing start time condition")
	}
	if n.stopTL == nil {
		err = errors.New("invalid query, missing stop time condition")
	}
	return n, err
}

// Set the dimensions on the query
func (q *Query) Dimensions(dims []interface{}) error {
	q.stmt.Dimensions = q.stmt.Dimensions[:0]
	q.groupByTimeDL = nil
	q.groupByOffsetDL = &influxql.DurationLiteral{
		Val: 0,
	}
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
			q.groupByTimeDL = &influxql.DurationLiteral{
				Val: dim,
			}
			if q.alignGroup {
				q.SetStartTime(q.StartTime())
			}
			q.stmt.Dimensions = append(q.stmt.Dimensions,
				&influxql.Dimension{
					Expr: &influxql.Call{
						Name: "time",
						Args: []influxql.Expr{
							q.groupByTimeDL,
							q.groupByOffsetDL,
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
		case *ast.StarNode:
			q.stmt.Dimensions = append(q.stmt.Dimensions,
				&influxql.Dimension{
					Expr: &influxql.Wildcard{},
				})
		case TimeDimension:
			if hasTime {
				return fmt.Errorf("groupBy cannot have more than one time dimension")
			}
			// Add time dimension
			hasTime = true
			q.groupByTimeDL = &influxql.DurationLiteral{
				Val: dim.Length,
			}
			q.groupByOffsetDL.Val = dim.Offset
			if q.alignGroup {
				q.SetStartTime(q.StartTime())
			}
			q.stmt.Dimensions = append(q.stmt.Dimensions,
				&influxql.Dimension{
					Expr: &influxql.Call{
						Name: "time",
						Args: []influxql.Expr{
							q.groupByTimeDL,
							q.groupByOffsetDL,
						},
					},
				})

		default:
			return fmt.Errorf("invalid dimension type:%T, must be string or time.Duration", d)
		}
	}

	return nil
}

func (q *Query) IsGroupedByTime() bool {
	return q.groupByTimeDL != nil
}

func (q *Query) AlignGroup() {
	q.alignGroup = true
}

func (q *Query) Fill(option influxql.FillOption, value interface{}) {
	q.stmt.Fill = option
	q.stmt.FillValue = value
}

func (q *Query) String() string {
	return q.stmt.String()
}

type TimeDimension struct {
	Length time.Duration
	Offset time.Duration
}

func groupByTime(length time.Duration, offset ...time.Duration) (TimeDimension, error) {
	var o time.Duration
	if l := len(offset); l == 1 {
		o = offset[0]

	} else if l != 0 {
		return TimeDimension{}, fmt.Errorf("time() function expects 1 or 2 args, got %d", l+1)
	}
	return TimeDimension{
		Length: length,
		Offset: o,
	}, nil
}
