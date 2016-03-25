package pipeline

import "github.com/influxdata/influxdb/influxql"

// tmpl -- go get github.com/benbjohnson/tmpl
//go:generate tmpl -data=@../tmpldata influxql.gen.go.tmpl

// An InfluxQLNode performs the available function from the InfluxQL language.
// These function can be performed on a stream or batch edge.
// The resulting edge is dependent on the function.
// For a stream edge all points with the same time are accumulated into the function.
// For a batch edge all points in the batch are accumulated into the function.
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
	chainnode

	// tick:ignore
	Method string
	// tick:ignore
	Field string

	// The name of the field, defaults to the name of
	// function used (i.e. .mean -> 'mean')
	As string

	// tick:ignore
	ReduceCreater ReduceCreater

	// tick:ignore
	PointTimes bool `tick:"UsePointTimes"`
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
		CreateFloatIntegerReducer: func() (influxql.FloatPointAggregator, influxql.IntegerPointEmitter) {
			fn := influxql.NewFloatFuncIntegerReducer(influxql.FloatCountReduce)
			return fn, fn
		},
		CreateIntegerReducer: func() (influxql.IntegerPointAggregator, influxql.IntegerPointEmitter) {
			fn := influxql.NewIntegerFuncReducer(influxql.IntegerCountReduce)
			return fn, fn
		},
	})
	n.linkChild(i)
	return i
}

// Produce batch of only the distinct points.
func (n *chainnode) Distinct(field string) *InfluxQLNode {
	i := newInfluxQLNode("distinct", field, n.Provides(), BatchEdge, ReduceCreater{
		CreateFloatBulkReducer: func() (FloatBulkPointAggregator, influxql.FloatPointEmitter) {
			fn := influxql.NewFloatSliceFuncReducer(influxql.FloatDistinctReduceSlice)
			return fn, fn
		},
		CreateIntegerBulkReducer: func() (IntegerBulkPointAggregator, influxql.IntegerPointEmitter) {
			fn := influxql.NewIntegerSliceFuncReducer(influxql.IntegerDistinctReduceSlice)
			return fn, fn
		},
	})
	n.linkChild(i)
	return i
}

// Compute the mean of the data.
func (n *chainnode) Mean(field string) *InfluxQLNode {
	i := newInfluxQLNode("mean", field, n.Provides(), StreamEdge, ReduceCreater{
		CreateFloatReducer: func() (influxql.FloatPointAggregator, influxql.FloatPointEmitter) {
			fn := influxql.NewFloatMeanReducer()
			return fn, fn
		},
		CreateIntegerFloatReducer: func() (influxql.IntegerPointAggregator, influxql.FloatPointEmitter) {
			fn := influxql.NewIntegerMeanReducer()
			return fn, fn
		},
	})
	n.linkChild(i)
	return i
}

// Compute the median of the data. Note, this method is not a selector,
// if you want the median point use .percentile(field, 50.0).
func (n *chainnode) Median(field string) *InfluxQLNode {
	i := newInfluxQLNode("median", field, n.Provides(), StreamEdge, ReduceCreater{
		CreateFloatBulkReducer: func() (FloatBulkPointAggregator, influxql.FloatPointEmitter) {
			fn := influxql.NewFloatSliceFuncReducer(influxql.FloatMedianReduceSlice)
			return fn, fn
		},
		CreateIntegerBulkFloatReducer: func() (IntegerBulkPointAggregator, influxql.FloatPointEmitter) {
			fn := influxql.NewIntegerSliceFuncFloatReducer(influxql.IntegerMedianReduceSlice)
			return fn, fn
		},
	})
	n.linkChild(i)
	return i
}

// Compute the difference between min and max points.
func (n *chainnode) Spread(field string) *InfluxQLNode {
	i := newInfluxQLNode("spread", field, n.Provides(), StreamEdge, ReduceCreater{
		CreateFloatBulkReducer: func() (FloatBulkPointAggregator, influxql.FloatPointEmitter) {
			fn := influxql.NewFloatSliceFuncReducer(influxql.FloatSpreadReduceSlice)
			return fn, fn
		},
		CreateIntegerBulkReducer: func() (IntegerBulkPointAggregator, influxql.IntegerPointEmitter) {
			fn := influxql.NewIntegerSliceFuncReducer(influxql.IntegerSpreadReduceSlice)
			return fn, fn
		},
	})
	n.linkChild(i)
	return i
}

// Compute the sum of all values.
func (n *chainnode) Sum(field string) *InfluxQLNode {
	i := newInfluxQLNode("sum", field, n.Provides(), StreamEdge, ReduceCreater{
		CreateFloatReducer: func() (influxql.FloatPointAggregator, influxql.FloatPointEmitter) {
			fn := influxql.NewFloatFuncReducer(influxql.FloatSumReduce)
			return fn, fn
		},
		CreateIntegerReducer: func() (influxql.IntegerPointAggregator, influxql.IntegerPointEmitter) {
			fn := influxql.NewIntegerFuncReducer(influxql.IntegerSumReduce)
			return fn, fn
		},
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
		CreateFloatReducer: func() (influxql.FloatPointAggregator, influxql.FloatPointEmitter) {
			fn := influxql.NewFloatFuncReducer(influxql.FloatFirstReduce)
			return fn, fn
		},
		CreateIntegerReducer: func() (influxql.IntegerPointAggregator, influxql.IntegerPointEmitter) {
			fn := influxql.NewIntegerFuncReducer(influxql.IntegerFirstReduce)
			return fn, fn
		},
	})
	n.linkChild(i)
	return i
}

// Select the last point.
func (n *chainnode) Last(field string) *InfluxQLNode {
	i := newInfluxQLNode("last", field, n.Provides(), StreamEdge, ReduceCreater{
		CreateFloatReducer: func() (influxql.FloatPointAggregator, influxql.FloatPointEmitter) {
			fn := influxql.NewFloatFuncReducer(influxql.FloatLastReduce)
			return fn, fn
		},
		CreateIntegerReducer: func() (influxql.IntegerPointAggregator, influxql.IntegerPointEmitter) {
			fn := influxql.NewIntegerFuncReducer(influxql.IntegerLastReduce)
			return fn, fn
		},
	})
	n.linkChild(i)
	return i
}

// Select the minimum point.
func (n *chainnode) Min(field string) *InfluxQLNode {
	i := newInfluxQLNode("min", field, n.Provides(), StreamEdge, ReduceCreater{
		CreateFloatReducer: func() (influxql.FloatPointAggregator, influxql.FloatPointEmitter) {
			fn := influxql.NewFloatFuncReducer(influxql.FloatMinReduce)
			return fn, fn
		},
		CreateIntegerReducer: func() (influxql.IntegerPointAggregator, influxql.IntegerPointEmitter) {
			fn := influxql.NewIntegerFuncReducer(influxql.IntegerMinReduce)
			return fn, fn
		},
	})
	n.linkChild(i)
	return i
}

// Select the maximum point.
func (n *chainnode) Max(field string) *InfluxQLNode {
	i := newInfluxQLNode("max", field, n.Provides(), StreamEdge, ReduceCreater{
		CreateFloatReducer: func() (influxql.FloatPointAggregator, influxql.FloatPointEmitter) {
			fn := influxql.NewFloatFuncReducer(influxql.FloatMaxReduce)
			return fn, fn
		},
		CreateIntegerReducer: func() (influxql.IntegerPointAggregator, influxql.IntegerPointEmitter) {
			fn := influxql.NewIntegerFuncReducer(influxql.IntegerMaxReduce)
			return fn, fn
		},
	})
	n.linkChild(i)
	return i
}

// Select a point at the given percentile. This is a selector function, no interpolation between points is performed.
func (n *chainnode) Percentile(field string, percentile float64) *InfluxQLNode {
	i := newInfluxQLNode("percentile", field, n.Provides(), StreamEdge, ReduceCreater{
		CreateFloatBulkReducer: func() (FloatBulkPointAggregator, influxql.FloatPointEmitter) {
			fn := influxql.NewFloatSliceFuncReducer(influxql.NewFloatPercentileReduceSliceFunc(percentile))
			return fn, fn
		},
		CreateIntegerBulkReducer: func() (IntegerBulkPointAggregator, influxql.IntegerPointEmitter) {
			fn := influxql.NewIntegerSliceFuncReducer(influxql.NewIntegerPercentileReduceSliceFunc(percentile))
			return fn, fn
		},
	})
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
		CreateFloatBulkReducer: func() (FloatBulkPointAggregator, influxql.FloatPointEmitter) {
			fn := influxql.NewFloatSliceFuncReducer(influxql.NewFloatTopReduceSliceFunc(
				int(num),
				tags,
				influxql.Interval{},
			))
			return fn, fn
		},
		CreateIntegerBulkReducer: func() (IntegerBulkPointAggregator, influxql.IntegerPointEmitter) {
			fn := influxql.NewIntegerSliceFuncReducer(influxql.NewIntegerTopReduceSliceFunc(
				int(num),
				tags,
				influxql.Interval{},
			))
			return fn, fn
		},
		TopBottomCallInfo: &TopBottomCallInfo{
			FieldsAndTags: fieldsAndTags,
		},
	})
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
		CreateFloatBulkReducer: func() (FloatBulkPointAggregator, influxql.FloatPointEmitter) {
			fn := influxql.NewFloatSliceFuncReducer(influxql.NewFloatBottomReduceSliceFunc(
				int(num),
				tags,
				influxql.Interval{},
			))
			return fn, fn
		},
		CreateIntegerBulkReducer: func() (IntegerBulkPointAggregator, influxql.IntegerPointEmitter) {
			fn := influxql.NewIntegerSliceFuncReducer(influxql.NewIntegerBottomReduceSliceFunc(
				int(num),
				tags,
				influxql.Interval{},
			))
			return fn, fn
		},
		TopBottomCallInfo: &TopBottomCallInfo{
			FieldsAndTags: fieldsAndTags,
		},
	})
	n.linkChild(i)
	return i
}

//------------------------------------
// Transformation Functions
//

// Compute the standard deviation.
func (n *chainnode) Stddev(field string) *InfluxQLNode {
	i := newInfluxQLNode("stddev", field, n.Provides(), StreamEdge, ReduceCreater{
		CreateFloatBulkReducer: func() (FloatBulkPointAggregator, influxql.FloatPointEmitter) {
			fn := influxql.NewFloatSliceFuncReducer(influxql.FloatStddevReduceSlice)
			return fn, fn
		},
		CreateIntegerBulkFloatReducer: func() (IntegerBulkPointAggregator, influxql.FloatPointEmitter) {
			fn := influxql.NewIntegerSliceFuncFloatReducer(influxql.IntegerStddevReduceSlice)
			return fn, fn
		},
	})
	n.linkChild(i)
	return i
}
