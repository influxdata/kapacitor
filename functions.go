package kapacitor

import (
	"github.com/influxdb/influxdb/tsdb"
	"github.com/influxdb/kapacitor/pipeline"
)

type influxqlMapReducers struct{}

func newInfluxQL() *influxqlMapReducers {
	return &influxqlMapReducers{}
}

func (influxqlMapReducers) Sum(field string) pipeline.MapReduceInfo {
	return mr(field, "sum", tsdb.MapSum, tsdb.ReduceSum)
}

func (influxqlMapReducers) Count(field string) pipeline.MapReduceInfo {
	return mr(field, "count", tsdb.MapCount, tsdb.ReduceSum)
}

func (influxqlMapReducers) Distinct(field string) pipeline.MapReduceInfo {
	return mr(field, "distinct", tsdb.MapDistinct, tsdb.ReduceDistinct)
}

func (influxqlMapReducers) Mean(field string) pipeline.MapReduceInfo {
	return mr(field, "mean", tsdb.MapMean, tsdb.ReduceMean)
}

func (influxqlMapReducers) Median(field string) pipeline.MapReduceInfo {
	return mr(field, "median", tsdb.MapStddev, tsdb.ReduceMedian)
}

func (influxqlMapReducers) Min(field string) pipeline.MapReduceInfo {
	m := func(in *tsdb.MapInput) interface{} {
		return tsdb.MapMin(in, field)
	}
	return mr(field, "min", m, tsdb.ReduceMin)
}

func (influxqlMapReducers) Max(field string) pipeline.MapReduceInfo {
	m := func(in *tsdb.MapInput) interface{} {
		return tsdb.MapMax(in, field)
	}
	return mr(field, "max", m, tsdb.ReduceMax)
}

func (influxqlMapReducers) Spread(field string) pipeline.MapReduceInfo {
	return mr(field, "spread", tsdb.MapSpread, tsdb.ReduceSpread)
}

func (influxqlMapReducers) Stddev(field string) pipeline.MapReduceInfo {
	return mr(field, "stddev", tsdb.MapStddev, tsdb.ReduceStddev)
}

func (influxqlMapReducers) First(field string) pipeline.MapReduceInfo {
	m := func(in *tsdb.MapInput) interface{} {
		return tsdb.MapFirst(in, field)
	}
	return mr(field, "first", m, tsdb.ReduceFirst)
}

func (influxqlMapReducers) Last(field string) pipeline.MapReduceInfo {
	m := func(in *tsdb.MapInput) interface{} {
		return tsdb.MapLast(in, field)
	}
	return mr(field, "last", m, tsdb.ReduceLast)
}

func (influxqlMapReducers) Percentile(field string, p float64) pipeline.MapReduceInfo {
	r := func(values []interface{}) interface{} {
		return tsdb.ReducePercentile(values, p)
	}
	return mr(field, "percentile", tsdb.MapEcho, r)
}

func (influxqlMapReducers) Top(field string, limit int64, fields ...string) pipeline.MapReduceInfo {
	m := func(in *tsdb.MapInput) interface{} {
		return tsdb.MapTopBottom(in, int(limit), fields, len(fields)+1, "top")
	}
	r := func(values []interface{}) interface{} {
		return tsdb.ReduceTopBottom(values, int(limit), fields, "top")
	}
	return mr(field, "top", m, r)
}

func (influxqlMapReducers) Bottom(field string, limit int64, fields ...string) pipeline.MapReduceInfo {
	m := func(in *tsdb.MapInput) interface{} {
		return tsdb.MapTopBottom(in, int(limit), fields, len(fields)+1, "bottom")
	}
	r := func(values []interface{}) interface{} {
		return tsdb.ReduceTopBottom(values, int(limit), fields, "bottom")
	}
	return mr(field, "bottom", m, r)
}

// -----------------
// helper methods

// create MapReduceInfo
func mr(field, newField string, m func(*tsdb.MapInput) interface{}, r func([]interface{}) interface{}) pipeline.MapReduceInfo {
	return pipeline.MapReduceInfo{
		Map: MapInfo{
			Field: field,
			Func:  m,
		},
		Reduce: reduce(r, newField),
	}
}

// wrap tsdb.reduceFunc for ReduceFunc
func reduce(f func([]interface{}) interface{}, field string) ReduceFunc {
	return ReduceFunc(func(in []interface{}) (string, interface{}) {
		v := f(in)
		if pp, ok := v.(tsdb.PositionPoint); ok {
			return field, pp.Value
		}
		if pp, ok := v.(tsdb.PositionPoints); ok {
			values := make([]interface{}, len(pp))
			for i := range pp {
				values[i] = pp[i].Value
			}
			return field, values
		}
		return field, v
	})
}
