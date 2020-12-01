package kapacitor

import (
	"sync"
	"time"
)

type QueryFlux struct {
	org     string
	orgID   string
	stmt    string
	queryMu sync.Mutex
	Now     time.Time
	//StartTime time.Time
	//StopTime  time.Time
}

func NewQueryFlux(queryString, org, orgID string) (*QueryFlux, error) {
	return &QueryFlux{org: org, orgID: orgID, stmt: queryString}, nil
}

//// Set the start time of the query
//func (q *QueryFlux) SetStartTime(s time.Time) {
//	q.StartTime = s
//}

////// Set the stop time of the query
//func (q *QueryFlux) SetStopTime(s time.Time) {
//	q.StopTime = s
//}

// Deep clone this query
func (q *QueryFlux) Clone() (*QueryFlux, error) {
	q.queryMu.Lock()
	defer q.queryMu.Unlock()
	return &QueryFlux{
		stmt:  q.stmt,
		org:   q.org,
		orgID: q.orgID,
	}, nil

}

//// Set the stop time of the query
//func (q *FluxQuery) Stop() time.Time {
//	return q.stopTime
//}
//
//// Set the start time of the query
//func (q *FluxQuery) SetStartTime(s time.Time) {
//	q.startTime = s
//}
//
//// Set the stop time of the query
//func (q *FluxQuery) SetStopTime(s time.Time) {
//	q.stopTime = s
//}

// // Set the dimensions on the query
//func (q *Query) Dimensions(dims []interface{}) error {
//	q.stmt.Dimensions = q.stmt.Dimensions[:0]
//	q.groupByTimeDL = nil
//	q.groupByOffsetDL = &influxql.DurationLiteral{
//		Val: 0,
//	}
//	// Add in dimensions
//	hasTime := false
//	for _, d := range dims {
//		switch dim := d.(type) {
//		case time.Duration:
//			if hasTime {
//				return fmt.Errorf("groupBy cannot have more than one time dimension")
//			}
//			// Add time dimension
//			hasTime = true
//			q.groupByTimeDL = &influxql.DurationLiteral{
//				Val: dim,
//			}
//			if q.alignGroup {
//				q.SetStartTime(q.StartTime())
//			}
//			q.stmt.Dimensions = append(q.stmt.Dimensions,
//				&influxql.Dimension{
//					Expr: &influxql.Call{
//						Name: "time",
//						Args: []influxql.Expr{
//							q.groupByTimeDL,
//							q.groupByOffsetDL,
//						},
//					},
//				})
//		case string:
//			q.stmt.Dimensions = append(q.stmt.Dimensions,
//				&influxql.Dimension{
//					Expr: &influxql.VarRef{
//						Val: dim,
//					},
//				})
//		case *ast.StarNode:
//			q.stmt.Dimensions = append(q.stmt.Dimensions,
//				&influxql.Dimension{
//					Expr: &influxql.Wildcard{},
//				})
//		case TimeDimension:
//			if hasTime {
//				return fmt.Errorf("groupBy cannot have more than one time dimension")
//			}
//			// Add time dimension
//			hasTime = true
//			q.groupByTimeDL = &influxql.DurationLiteral{
//				Val: dim.Length,
//			}
//			q.groupByOffsetDL.Val = dim.Offset
//			if q.alignGroup {
//				q.SetStartTime(q.StartTime())
//			}
//			q.stmt.Dimensions = append(q.stmt.Dimensions,
//				&influxql.Dimension{
//					Expr: &influxql.Call{
//						Name: "time",
//						Args: []influxql.Expr{
//							q.groupByTimeDL,
//							q.groupByOffsetDL,
//						},
//					},
//				})
//
//		default:
//			return fmt.Errorf("invalid dimension type:%T, must be string or time.Duration", d)
//		}
//	}
//
//	return nil
//}

//func (q *Query) IsGroupedByTime() bool {
//	return q.groupByTimeDL != nil
//}
//
//func (q *Query) AlignGroup() {
//	q.alignGroup = true
//}
//
//func (q *Query) Fill(option influxql.FillOption, value interface{}) {
//	q.stmt.Fill = option
//	q.stmt.FillValue = value
//}

func (q *QueryFlux) String() string {
	return q.stmt
}
