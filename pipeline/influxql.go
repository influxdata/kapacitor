package pipeline

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxql"
)

// tmpl -- go get github.com/benbjohnson/tmpl
//go:generate tmpl -data=@../tmpldata.json influxql.gen.go.tmpl

// An InfluxQLNode performs the available function from the InfluxQL language.
// These function can be performed on a stream or batch edge.
// The resulting edge is dependent on the function.
// For a stream edge, all points with the same time are accumulated into the function.
// For a batch edge, all points in the batch are accumulated into the function.
//
//
// Example:
//    stream
//        |window()
//            .period(10s)
//            .every(10s)
//        // Sum the values for each 10s window of data.
//        |sum('value')
//
//
// Note: Derivative has its own implementation as a DerivativeNode instead of as part of the
// InfluxQL functions.
type InfluxQLNode struct {
	chainnode `json:"-"`

	// tick:ignore
	Method string `json:"-"`
	// tick:ignore
	Field string `json:"field"`

	// The name of the field, defaults to the name of
	// function used (i.e. .mean -> 'mean')
	As string `json:"as"`

	// tick:ignore
	ReduceCreater ReduceCreater `json:"-"`

	// tick:ignore
	PointTimes bool `tick:"UsePointTimes" json:"usePointTimes"`

	//tick:ignore
	Reducer Node

	// tick:ignore
	Args []interface{} `json:"args"`
}

func newInfluxQLNode(method, field string, wants, provides EdgeType, reducer ReduceCreater) *InfluxQLNode {
	return &InfluxQLNode{
		chainnode:     newBasicChainNode(method, wants, provides),
		Method:        method,
		Field:         field,
		As:            method,
		ReduceCreater: reducer,
	}
}

// MarshalJSON converts InfluxQLNode to JSON
// tick:ignore
func (n *InfluxQLNode) MarshalJSON() ([]byte, error) {
	type Alias InfluxQLNode
	var raw = &struct {
		TypeOf
		*Alias
		Args []interface{} `json:"args"`
	}{
		TypeOf: TypeOf{
			Type: n.Method,
			ID:   n.ID(),
		},
		Alias: (*Alias)(n),
		Args:  n.Args,
	}
	for i, arg := range raw.Args {
		switch dur := arg.(type) {
		case time.Duration:
			raw.Args[i] = influxql.FormatDuration(dur)
		}
	}
	return json.Marshal(raw)
}

// UnmarshalJSON converts JSON to an InfluxQLNode
// tick:ignore
func (n *InfluxQLNode) UnmarshalJSON(data []byte) error {
	type Alias InfluxQLNode
	var raw = &struct {
		TypeOf
		*Alias
		Args []interface{} `json:"args"`
	}{
		Alias: (*Alias)(n),
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	err := dec.Decode(&raw)
	if err != nil {
		return err
	}
	switch raw.Type {
	case "count", "distinct", "mean", "median", "mode", "spread", "sum", "first":
	case "last", "min", "max", "stddev", "difference", "cumulativeSum":
	case "top", "bottom", "movingAverage":
		for i, arg := range raw.Args {
			switch num := arg.(type) {
			case json.Number:
				if raw.Args[i], err = num.Int64(); err != nil {
					return err
				}
			}
		}
	case "elapsed", "holtWinters", "holtWintersWithFit":
		for i, arg := range raw.Args {
			switch a := arg.(type) {
			case json.Number:
				if raw.Args[i], err = a.Int64(); err != nil {
					return err
				}
			case string:
				if raw.Args[i], err = influxql.ParseDuration(a); err != nil {
					return err
				}
			}
		}
	case "percentile":
		for i, arg := range raw.Args {
			switch a := arg.(type) {
			case json.Number:
				if raw.Args[i], err = a.Float64(); err != nil {
					return err
				}
			}
		}
	default:
		return fmt.Errorf("error unmarshaling node %d of type %s as InfluxQLNode", raw.ID, raw.Type)
	}

	for i, arg := range raw.Args {
		switch integer := arg.(type) {
		case int:
			raw.Args[i] = int64(integer)
		}
	}
	n.Args = raw.Args
	n.Method = raw.Type
	n.setID(raw.ID)
	return nil
}

// Use the time of the selected point instead of the time of the batch.
//
// Only applies to selector functions like first, last, top, bottom, etc.
// Aggregation functions always use the batch time.
// tick:property
func (n *InfluxQLNode) UsePointTimes() *InfluxQLNode {
	n.PointTimes = true
	return n
}

//------------------------------------
// Aggregation Functions
//

// Count the number of points.
func (n *chainnode) Count(field string) *InfluxQLNode {
	i := newInfluxQLNode("count", field, n.Provides(), StreamEdge, ReduceCreater{
		CreateFloatIntegerReducer: func() (query.FloatPointAggregator, query.IntegerPointEmitter) {
			fn := query.NewFloatFuncIntegerReducer(query.FloatCountReduce, &query.IntegerPoint{Value: 0})
			return fn, fn
		},
		CreateIntegerReducer: func() (query.IntegerPointAggregator, query.IntegerPointEmitter) {
			fn := query.NewIntegerFuncReducer(query.IntegerCountReduce, &query.IntegerPoint{Value: 0})
			return fn, fn
		},
		CreateStringIntegerReducer: func() (query.StringPointAggregator, query.IntegerPointEmitter) {
			fn := query.NewStringFuncIntegerReducer(query.StringCountReduce, &query.IntegerPoint{Value: 0})
			return fn, fn
		},
		CreateBooleanIntegerReducer: func() (query.BooleanPointAggregator, query.IntegerPointEmitter) {
			fn := query.NewBooleanFuncIntegerReducer(query.BooleanCountReduce, &query.IntegerPoint{Value: 0})
			return fn, fn
		},
		IsEmptyOK: true,
	})
	n.linkChild(i)
	return i
}

// Produce batch of only the distinct points.
func (n *chainnode) Distinct(field string) *InfluxQLNode {
	i := newInfluxQLNode("distinct", field, n.Provides(), BatchEdge, ReduceCreater{
		CreateFloatReducer: func() (query.FloatPointAggregator, query.FloatPointEmitter) {
			fn := query.NewFloatDistinctReducer()
			return fn, fn
		},
		CreateIntegerReducer: func() (query.IntegerPointAggregator, query.IntegerPointEmitter) {
			fn := query.NewIntegerDistinctReducer()
			return fn, fn
		},
		CreateStringReducer: func() (query.StringPointAggregator, query.StringPointEmitter) {
			fn := query.NewStringDistinctReducer()
			return fn, fn
		},
		CreateBooleanReducer: func() (query.BooleanPointAggregator, query.BooleanPointEmitter) {
			fn := query.NewBooleanDistinctReducer()
			return fn, fn
		},
	})
	n.linkChild(i)
	return i
}

// Compute the mean of the data.
func (n *chainnode) Mean(field string) *InfluxQLNode {
	i := newInfluxQLNode("mean", field, n.Provides(), StreamEdge, ReduceCreater{
		CreateFloatReducer: func() (query.FloatPointAggregator, query.FloatPointEmitter) {
			fn := query.NewFloatMeanReducer()
			return fn, fn
		},
		CreateIntegerFloatReducer: func() (query.IntegerPointAggregator, query.FloatPointEmitter) {
			fn := query.NewIntegerMeanReducer()
			return fn, fn
		},
	})
	n.linkChild(i)
	return i
}

// Compute the median of the data. Note, this method is not a selector,
// if you want the median point use `.percentile(field, 50.0)`.
func (n *chainnode) Median(field string) *InfluxQLNode {
	i := newInfluxQLNode("median", field, n.Provides(), StreamEdge, ReduceCreater{
		CreateFloatReducer: func() (query.FloatPointAggregator, query.FloatPointEmitter) {
			fn := query.NewFloatSliceFuncReducer(query.FloatMedianReduceSlice)
			return fn, fn
		},
		CreateIntegerFloatReducer: func() (query.IntegerPointAggregator, query.FloatPointEmitter) {
			fn := query.NewIntegerSliceFuncFloatReducer(query.IntegerMedianReduceSlice)
			return fn, fn
		},
	})
	n.linkChild(i)
	return i
}

// Compute the mode of the data.
func (n *chainnode) Mode(field string) *InfluxQLNode {
	i := newInfluxQLNode("mode", field, n.Provides(), StreamEdge, ReduceCreater{
		CreateFloatReducer: func() (query.FloatPointAggregator, query.FloatPointEmitter) {
			fn := query.NewFloatSliceFuncReducer(query.FloatModeReduceSlice)
			return fn, fn
		},
		CreateIntegerReducer: func() (query.IntegerPointAggregator, query.IntegerPointEmitter) {
			fn := query.NewIntegerSliceFuncReducer(query.IntegerModeReduceSlice)
			return fn, fn
		},
	})
	n.linkChild(i)
	return i
}

// Compute the difference between `min` and `max` points.
func (n *chainnode) Spread(field string) *InfluxQLNode {
	i := newInfluxQLNode("spread", field, n.Provides(), StreamEdge, ReduceCreater{
		CreateFloatReducer: func() (query.FloatPointAggregator, query.FloatPointEmitter) {
			// fn := NewFloatSliceFuncReducer(query.FloatSpreadReduceSlice)
			fn := query.NewFloatSpreadReducer()
			return fn, fn
		},
		CreateIntegerReducer: func() (query.IntegerPointAggregator, query.IntegerPointEmitter) {
			//fn := query.NewIntegerSliceFuncReducer(query.IntegerSpreadReduceSlice)
			fn := query.NewIntegerSpreadReducer()
			return fn, fn
		},
	})
	n.linkChild(i)
	return i
}

// Compute the sum of all values.
func (n *chainnode) Sum(field string) *InfluxQLNode {
	i := newInfluxQLNode("sum", field, n.Provides(), StreamEdge, ReduceCreater{
		CreateFloatReducer: func() (query.FloatPointAggregator, query.FloatPointEmitter) {
			fn := query.NewFloatFuncReducer(query.FloatSumReduce, &query.FloatPoint{Value: 0})
			return fn, fn
		},
		CreateIntegerReducer: func() (query.IntegerPointAggregator, query.IntegerPointEmitter) {
			fn := query.NewIntegerFuncReducer(query.IntegerSumReduce, &query.IntegerPoint{Value: 0})
			return fn, fn
		},
		IsEmptyOK: true,
	})
	n.linkChild(i)
	return i
}

//------------------------------------
// Selection Functions
//

// Select the first point.
func (n *chainnode) First(field string) *InfluxQLNode {
	i := newInfluxQLNode("first", field, n.Provides(), StreamEdge, ReduceCreater{
		CreateFloatReducer: func() (query.FloatPointAggregator, query.FloatPointEmitter) {
			fn := query.NewFloatFuncReducer(query.FloatFirstReduce, nil)
			return fn, fn
		},
		CreateIntegerReducer: func() (query.IntegerPointAggregator, query.IntegerPointEmitter) {
			fn := query.NewIntegerFuncReducer(query.IntegerFirstReduce, nil)
			return fn, fn
		},
		CreateStringReducer: func() (query.StringPointAggregator, query.StringPointEmitter) {
			fn := query.NewStringFuncReducer(query.StringFirstReduce, nil)
			return fn, fn
		},
		CreateBooleanReducer: func() (query.BooleanPointAggregator, query.BooleanPointEmitter) {
			fn := query.NewBooleanFuncReducer(query.BooleanFirstReduce, nil)
			return fn, fn
		},
		IsSimpleSelector: true,
	})
	n.linkChild(i)
	return i
}

// Select the last point.
func (n *chainnode) Last(field string) *InfluxQLNode {
	i := newInfluxQLNode("last", field, n.Provides(), StreamEdge, ReduceCreater{
		CreateFloatReducer: func() (query.FloatPointAggregator, query.FloatPointEmitter) {
			fn := query.NewFloatFuncReducer(query.FloatLastReduce, nil)
			return fn, fn
		},
		CreateIntegerReducer: func() (query.IntegerPointAggregator, query.IntegerPointEmitter) {
			fn := query.NewIntegerFuncReducer(query.IntegerLastReduce, nil)
			return fn, fn
		},
		CreateStringReducer: func() (query.StringPointAggregator, query.StringPointEmitter) {
			fn := query.NewStringFuncReducer(query.StringLastReduce, nil)
			return fn, fn
		},
		CreateBooleanReducer: func() (query.BooleanPointAggregator, query.BooleanPointEmitter) {
			fn := query.NewBooleanFuncReducer(query.BooleanLastReduce, nil)
			return fn, fn
		},
		IsSimpleSelector: true,
	})
	n.linkChild(i)
	return i
}

// Select the minimum point.
func (n *chainnode) Min(field string) *InfluxQLNode {
	i := newInfluxQLNode("min", field, n.Provides(), StreamEdge, ReduceCreater{
		CreateFloatReducer: func() (query.FloatPointAggregator, query.FloatPointEmitter) {
			fn := query.NewFloatFuncReducer(query.FloatMinReduce, nil)
			return fn, fn
		},
		CreateIntegerReducer: func() (query.IntegerPointAggregator, query.IntegerPointEmitter) {
			fn := query.NewIntegerFuncReducer(query.IntegerMinReduce, nil)
			return fn, fn
		},
		IsSimpleSelector: true,
	})
	n.linkChild(i)
	return i
}

// Select the maximum point.
func (n *chainnode) Max(field string) *InfluxQLNode {
	i := newInfluxQLNode("max", field, n.Provides(), StreamEdge, ReduceCreater{
		CreateFloatReducer: func() (query.FloatPointAggregator, query.FloatPointEmitter) {
			fn := query.NewFloatFuncReducer(query.FloatMaxReduce, nil)
			return fn, fn
		},
		CreateIntegerReducer: func() (query.IntegerPointAggregator, query.IntegerPointEmitter) {
			fn := query.NewIntegerFuncReducer(query.IntegerMaxReduce, nil)
			return fn, fn
		},
		IsSimpleSelector: true,
	})
	n.linkChild(i)
	return i
}

// Select a point at the given percentile. This is a selector function, no interpolation between points is performed.
func (n *chainnode) Percentile(field string, percentile float64) *InfluxQLNode {
	i := newInfluxQLNode("percentile", field, n.Provides(), StreamEdge, ReduceCreater{
		CreateFloatReducer: func() (query.FloatPointAggregator, query.FloatPointEmitter) {
			fn := query.NewFloatSliceFuncReducer(query.NewFloatPercentileReduceSliceFunc(percentile))
			return fn, fn
		},
		CreateIntegerReducer: func() (query.IntegerPointAggregator, query.IntegerPointEmitter) {
			fn := query.NewIntegerSliceFuncReducer(query.NewIntegerPercentileReduceSliceFunc(percentile))
			return fn, fn
		},
		IsSimpleSelector: true,
	})
	i.Args = []interface{}{percentile}
	n.linkChild(i)
	return i
}

//tick:ignore
type TopBottomCallInfo struct {
	FieldsAndTags []string
}

// Select the top `num` points for `field` and sort by any extra tags or fields.
func (n *chainnode) Top(num int64, field string, fieldsAndTags ...string) *InfluxQLNode {
	tags := make([]int, len(fieldsAndTags))
	for i := range fieldsAndTags {
		tags[i] = i
	}
	i := newInfluxQLNode("top", field, n.Provides(), BatchEdge, ReduceCreater{
		CreateFloatReducer: func() (query.FloatPointAggregator, query.FloatPointEmitter) {
			fn := query.NewFloatTopReducer(int(num))
			return fn, fn
		},
		CreateIntegerReducer: func() (query.IntegerPointAggregator, query.IntegerPointEmitter) {
			fn := query.NewIntegerTopReducer(int(num))
			return fn, fn
		},
		TopBottomCallInfo: &TopBottomCallInfo{
			FieldsAndTags: fieldsAndTags,
		},
	})
	i.Args = []interface{}{num}
	for _, ft := range fieldsAndTags {
		i.Args = append(i.Args, ft)
	}
	n.linkChild(i)
	return i
}

// Select the bottom `num` points for `field` and sort by any extra tags or fields.
func (n *chainnode) Bottom(num int64, field string, fieldsAndTags ...string) *InfluxQLNode {
	tags := make([]int, len(fieldsAndTags))
	for i := range fieldsAndTags {
		tags[i] = i
	}
	i := newInfluxQLNode("bottom", field, n.Provides(), BatchEdge, ReduceCreater{
		CreateFloatReducer: func() (query.FloatPointAggregator, query.FloatPointEmitter) {
			fn := query.NewFloatBottomReducer(int(num))
			return fn, fn
		},
		CreateIntegerReducer: func() (query.IntegerPointAggregator, query.IntegerPointEmitter) {
			fn := query.NewIntegerBottomReducer(int(num))
			return fn, fn
		},
		TopBottomCallInfo: &TopBottomCallInfo{
			FieldsAndTags: fieldsAndTags,
		},
	})
	i.Args = []interface{}{num}
	for _, ft := range fieldsAndTags {
		i.Args = append(i.Args, ft)
	}
	n.linkChild(i)
	return i
}

//------------------------------------
// Transformation Functions
//

// Compute the standard deviation.
func (n *chainnode) Stddev(field string) *InfluxQLNode {
	i := newInfluxQLNode("stddev", field, n.Provides(), StreamEdge, ReduceCreater{
		CreateFloatReducer: func() (query.FloatPointAggregator, query.FloatPointEmitter) {
			fn := query.NewFloatSliceFuncReducer(query.FloatStddevReduceSlice)
			return fn, fn
		},
		CreateIntegerFloatReducer: func() (query.IntegerPointAggregator, query.FloatPointEmitter) {
			fn := query.NewIntegerSliceFuncFloatReducer(query.IntegerStddevReduceSlice)
			return fn, fn
		},
	})
	n.linkChild(i)
	return i
}

// Compute the elapsed time between points
func (n *chainnode) Elapsed(field string, unit time.Duration) *InfluxQLNode {
	i := newInfluxQLNode("elapsed", field, n.Provides(), n.Provides(), ReduceCreater{
		CreateFloatIntegerReducer: func() (query.FloatPointAggregator, query.IntegerPointEmitter) {
			fn := query.NewFloatElapsedReducer(query.Interval{Duration: unit})
			return fn, fn
		},
		CreateIntegerReducer: func() (query.IntegerPointAggregator, query.IntegerPointEmitter) {
			fn := query.NewIntegerElapsedReducer(query.Interval{Duration: unit})
			return fn, fn
		},
		CreateStringIntegerReducer: func() (query.StringPointAggregator, query.IntegerPointEmitter) {
			fn := query.NewStringElapsedReducer(query.Interval{Duration: unit})
			return fn, fn
		},
		CreateBooleanIntegerReducer: func() (query.BooleanPointAggregator, query.IntegerPointEmitter) {
			fn := query.NewBooleanElapsedReducer(query.Interval{Duration: unit})
			return fn, fn
		},
		IsStreamTransformation: true,
	})
	i.Args = []interface{}{unit}
	n.linkChild(i)
	return i
}

// Compute the difference between points independent of elapsed time.
func (n *chainnode) Difference(field string) *InfluxQLNode {
	i := newInfluxQLNode("difference", field, n.Provides(), n.Provides(), ReduceCreater{
		CreateFloatReducer: func() (query.FloatPointAggregator, query.FloatPointEmitter) {
			fn := query.NewFloatDifferenceReducer(false)
			return fn, fn
		},
		CreateIntegerReducer: func() (query.IntegerPointAggregator, query.IntegerPointEmitter) {
			fn := query.NewIntegerDifferenceReducer(false)
			return fn, fn
		},
		IsStreamTransformation: true,
	})
	n.linkChild(i)
	return i
}

// Compute a moving average of the last window points.
// No points are emitted until the window is full.
func (n *chainnode) MovingAverage(field string, window int64) *InfluxQLNode {
	i := newInfluxQLNode("movingAverage", field, n.Provides(), n.Provides(), ReduceCreater{
		CreateFloatReducer: func() (query.FloatPointAggregator, query.FloatPointEmitter) {
			fn := query.NewFloatMovingAverageReducer(int(window))
			return fn, fn
		},
		CreateIntegerFloatReducer: func() (query.IntegerPointAggregator, query.FloatPointEmitter) {
			fn := query.NewIntegerMovingAverageReducer(int(window))
			return fn, fn
		},
		IsStreamTransformation: true,
	})
	i.Args = []interface{}{window}
	n.linkChild(i)
	return i
}

// Compute the holt-winters (https://docs.influxdata.com/influxdb/latest/query_language/functions/#holt-winters) forecast of a data set.
func (n *chainnode) HoltWinters(field string, h, m int64, interval time.Duration) *InfluxQLNode {
	return n.holtWinters(field, h, m, interval, false)
}

// Compute the holt-winters (https://docs.influxdata.com/influxdb/latest/query_language/functions/#holt-winters) forecast of a data set.
// This method also outputs all the points used to fit the data in addition to the forecasted data.
func (n *chainnode) HoltWintersWithFit(field string, h, m int64, interval time.Duration) *InfluxQLNode {
	return n.holtWinters(field, h, m, interval, true)
}

func (n *chainnode) holtWinters(field string, h, m int64, interval time.Duration, includeFitData bool) *InfluxQLNode {
	i := newInfluxQLNode("holtWinters", field, n.Provides(), BatchEdge, ReduceCreater{
		CreateFloatReducer: func() (query.FloatPointAggregator, query.FloatPointEmitter) {
			fn := query.NewFloatHoltWintersReducer(int(h), int(m), includeFitData, interval)
			return fn, fn
		},
		CreateIntegerFloatReducer: func() (query.IntegerPointAggregator, query.FloatPointEmitter) {
			fn := query.NewFloatHoltWintersReducer(int(h), int(m), includeFitData, interval)
			return fn, fn
		},
	})
	// Always use point times for Holt Winters
	i.PointTimes = true
	i.Args = []interface{}{h, m, interval, includeFitData}
	n.linkChild(i)
	return i
}

// Compute a cumulative sum of each point that is received.
// A point is emitted for every point collected.
func (n *chainnode) CumulativeSum(field string) *InfluxQLNode {
	i := newInfluxQLNode("cumulativeSum", field, n.Provides(), n.Provides(), ReduceCreater{
		CreateFloatReducer: func() (query.FloatPointAggregator, query.FloatPointEmitter) {
			fn := query.NewFloatCumulativeSumReducer()
			return fn, fn
		},
		CreateIntegerReducer: func() (query.IntegerPointAggregator, query.IntegerPointEmitter) {
			fn := query.NewIntegerCumulativeSumReducer()
			return fn, fn
		},
		IsStreamTransformation: true,
	})
	n.linkChild(i)
	return i
}
