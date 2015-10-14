package kapacitor

import (
	"github.com/influxdb/influxdb/tsdb"
	"github.com/influxdb/kapacitor/pipeline"
)

type influxqlMapReducers struct {
	Map    influxqlMappers
	Reduce influxqlReducers
	Sum,
	Count,
	Distinct,
	Mean,
	Median,
	//Min,
	//Max,
	Spread,
	Stddev pipeline.MapReduceFunc
	//First,
	//Last pipeline.MapReduceFunc
}

type influxqlMappers struct {
	Sum,
	Count,
	Distinct,
	Mean,
	Median,
	//Min,
	//Max,
	Spread,
	Stddev MapFunc
	//First,
	//Last MapFunc
}

type influxqlReducers struct {
	Sum,
	Count,
	Distinct,
	Mean,
	Median,
	//Min,
	//Max,
	Spread,
	Stddev ReduceFunc
	//First,
	//Last ReduceFunc
}

func newInfluxQL() *influxqlMapReducers {
	return &influxqlMapReducers{
		Map: influxqlMappers{
			Sum:      mapSum,
			Count:    mapCount,
			Distinct: mapDistinct,
			Mean:     mapMean,
			Median:   mapStddev,
			//Min:      mapMin,
			//Max:      mapMax,
			Spread: mapSpread,
			Stddev: mapStddev,
			//First:    mapFirst,
			//Last:     mapLast,
		},
		Reduce: influxqlReducers{
			Sum:      reduceSum,
			Count:    reduceCount,
			Distinct: reduceDistinct,
			Mean:     reduceMean,
			Median:   reduceMedian,
			//Min:      reduceMin,
			//Max:      reduceMax,
			Spread: reduceSpread,
			Stddev: reduceStddev,
			//First:    reduceFirst,
			//Last:     reduceLast,
		},
		Sum:      mrSum,
		Count:    mrCount,
		Distinct: mrDistinct,
		Mean:     mrMean,
		Median:   mrMedian,
		//Min:      mrMin,
		//Max:      mrMax,
		Spread: mrSpread,
		Stddev: mrStddev,
		//First:    mrFirst,
		//Last:     mrLast,
	}
}

// sum
var reduceSum = reduce(tsdb.ReduceSum, "sum")
var mapSum = MapFunc(tsdb.MapSum)
var mrSum = mr(mapSum, reduceSum)

// count
var reduceCount = reduce(tsdb.ReduceSum, "count")
var mapCount = MapFunc(tsdb.MapCount)
var mrCount = mr(mapCount, reduceCount)

// distinct
var reduceDistinct = reduce(tsdb.ReduceDistinct, "distinct")
var mapDistinct = MapFunc(tsdb.MapDistinct)
var mrDistinct = mr(mapDistinct, reduceDistinct)

// mean
var reduceMean = reduce(tsdb.ReduceMean, "mean")
var mapMean = MapFunc(tsdb.MapMean)
var mrMean = mr(mapMean, reduceMean)

// median
var reduceMedian = reduce(tsdb.ReduceMedian, "median")
var mrMedian = mr(mapStddev, reduceMedian)

// min
//var reduceMin = reduce(tsdb.ReduceMin, "min")
//var mapMin = MapFunc(tsdb.MapMin)
//var mrMin = mr(mapMin, reduceMin)

// max
//var reduceMax = reduce(tsdb.ReduceMax, "max")
//var mapMax = MapFunc(tsdb.MapMax)
//var mrMax = mr(mapMax, reduceMax)

// spread
var reduceSpread = reduce(tsdb.ReduceSpread, "spread")
var mapSpread = MapFunc(tsdb.MapSpread)
var mrSpread = mr(mapSpread, reduceSpread)

// stddev
var reduceStddev = reduce(tsdb.ReduceStddev, "stddev")
var mapStddev = MapFunc(tsdb.MapStddev)
var mrStddev = mr(mapStddev, reduceStddev)

// first
//var reduceFirst = reduce(tsdb.ReduceFirst, "first")
//var mapFirst = MapFunc(tsdb.MapFirst)
//var mrFirst = mr(mapFirst, reduceFirst)

// last
//var reduceLast = reduce(tsdb.ReduceLast, "last")
//var mapLast = MapFunc(tsdb.MapLast)
//var mrLast = mr(mapLast, reduceLast)

// top
//var reduceTop = reduce(tsdb.ReduceTop,  "top")
//var mapTop = MapFunc(tsdb.MapTopBottom)
//var mrTop = mr(mapTop, reduceTop)
//
//// bottom
//var reduceBottom = reduce(tsdb.ReduceBottom,  "bottom")
//var mapBottom = MapFunc(tsdb.MapBottom)
//var mrBottom = mr(mapBottom, reduceBottom)
//
//// percentile
//var reducePercentile = reduce(tsdb.ReducePercentile,  "percentile")
//var mapPercentile = MapFunc(tsdb.MapEcho)
//var mrPercentile = mr(mapPercentile, reducePercentile)
//
//// derivative
//var reduceDerivative = reduce(tsdb.ReduceDerivative,  "derivative")
//var mapDerivative = MapFunc(tsdb.MapDerivative)
//var mrDerivative = mr(mapDerivative, reduceDerivative)

func mr(m MapFunc, r ReduceFunc) pipeline.MapReduceFunc {
	return pipeline.MapReduceFunc(func() (interface{}, interface{}) {
		return m, r
	})
}

func reduce(f func([]interface{}) interface{}, field string) ReduceFunc {
	return ReduceFunc(func(in []interface{}) (string, interface{}) {
		v := f(in)
		return field, v
	})
}
