package kapacitor

import (
	"fmt"
	"time"

	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdb/influxdb/tsdb"
)

type influxqlMapReducers struct{}

func newInfluxQL() *influxqlMapReducers {
	return &influxqlMapReducers{}
}

func (influxqlMapReducers) Sum(field string) pipeline.MapReduceInfo {
	return mr(field, "sum", pipeline.StreamEdge, tsdb.MapSum, tsdb.ReduceSum)
}

func (influxqlMapReducers) Count(field string) pipeline.MapReduceInfo {
	return mr(field, "count", pipeline.StreamEdge, tsdb.MapCount, tsdb.ReduceSum)
}

func (influxqlMapReducers) Distinct(field string) pipeline.MapReduceInfo {
	return mr(field, "distinct", pipeline.BatchEdge, tsdb.MapDistinct, tsdb.ReduceDistinct)
}

func (influxqlMapReducers) Mean(field string) pipeline.MapReduceInfo {
	return mr(field, "mean", pipeline.StreamEdge, tsdb.MapMean, tsdb.ReduceMean)
}

func (influxqlMapReducers) Median(field string) pipeline.MapReduceInfo {
	return mr(field, "median", pipeline.StreamEdge, tsdb.MapStddev, tsdb.ReduceMedian)
}

func (influxqlMapReducers) Min(field string) pipeline.MapReduceInfo {
	m := func(in *tsdb.MapInput) interface{} {
		return tsdb.MapMin(in, field)
	}
	return mr(field, "min", pipeline.StreamEdge, m, tsdb.ReduceMin)
}

func (influxqlMapReducers) Max(field string) pipeline.MapReduceInfo {
	m := func(in *tsdb.MapInput) interface{} {
		return tsdb.MapMax(in, field)
	}
	return mr(field, "max", pipeline.StreamEdge, m, tsdb.ReduceMax)
}

func (influxqlMapReducers) Spread(field string) pipeline.MapReduceInfo {
	return mr(field, "spread", pipeline.StreamEdge, tsdb.MapSpread, tsdb.ReduceSpread)
}

func (influxqlMapReducers) Stddev(field string) pipeline.MapReduceInfo {
	return mr(field, "stddev", pipeline.StreamEdge, tsdb.MapStddev, tsdb.ReduceStddev)
}

func (influxqlMapReducers) First(field string) pipeline.MapReduceInfo {
	m := func(in *tsdb.MapInput) interface{} {
		return tsdb.MapFirst(in, field)
	}
	return mr(field, "first", pipeline.StreamEdge, m, tsdb.ReduceFirst)
}

func (influxqlMapReducers) Last(field string) pipeline.MapReduceInfo {
	m := func(in *tsdb.MapInput) interface{} {
		return tsdb.MapLast(in, field)
	}
	return mr(field, "last", pipeline.StreamEdge, m, tsdb.ReduceLast)
}

func (influxqlMapReducers) Percentile(field string, p float64) pipeline.MapReduceInfo {
	r := func(values []interface{}) interface{} {
		return tsdb.ReducePercentile(values, p)
	}
	return mr(field, "percentile", pipeline.StreamEdge, tsdb.MapEcho, r)
}

func (influxqlMapReducers) Top(limit int64, field string, fieldsAndTags ...string) pipeline.MapReduceInfo {
	m := func(in *tsdb.MapInput) interface{} {
		return tsdb.MapTopBottom(in, int(limit), fieldsAndTags, len(fieldsAndTags)+2, "top")
	}
	r := func(values []interface{}) interface{} {
		return tsdb.ReduceTopBottom(values, int(limit), fieldsAndTags, "top")
	}
	return mr(field, "top", pipeline.BatchEdge, m, r)
}

func (influxqlMapReducers) Bottom(limit int64, field string, fieldsAndTags ...string) pipeline.MapReduceInfo {
	m := func(in *tsdb.MapInput) interface{} {
		return tsdb.MapTopBottom(in, int(limit), fieldsAndTags, len(fieldsAndTags)+2, "bottom")
	}
	r := func(values []interface{}) interface{} {
		return tsdb.ReduceTopBottom(values, int(limit), fieldsAndTags, "bottom")
	}
	return mr(field, "bottom", pipeline.BatchEdge, m, r)
}

// -----------------
// helper methods

// create MapReduceInfo
func mr(field, newField string, et pipeline.EdgeType, m func(*tsdb.MapInput) interface{}, r func([]interface{}) interface{}) pipeline.MapReduceInfo {
	return pipeline.MapReduceInfo{
		Map: MapInfo{
			Field: field,
			Func:  m,
		},
		Reduce: reduce(r, newField, et),
		Edge:   et,
	}
}

// wrap tsdb.reduceFunc for ReduceFunc
func reduce(f func([]interface{}) interface{}, field string, et pipeline.EdgeType) ReduceFunc {
	return ReduceFunc(func(in []interface{}, tmax time.Time, usePointTimes bool, as string) interface{} {
		v := f(in)
		if as != "" {
			field = as
		}
		switch et {
		case pipeline.StreamEdge:
			return reduceResultToPoint(field, v, tmax, usePointTimes)
		case pipeline.BatchEdge:
			return reduceResultToBatch(field, v, tmax, usePointTimes)
		default:
			panic(fmt.Errorf("unknown edge type %v", et))
		}
	})
}

// take the result of a reduce operation and convert it to a point
func reduceResultToPoint(field string, v interface{}, tmax time.Time, usePointTimes bool) models.Point {
	if pp, ok := v.(tsdb.PositionPoint); ok {
		p := models.Point{}
		if usePointTimes {
			p.Time = time.Unix(pp.Time, 0).UTC()
		} else {
			p.Time = tmax
		}
		p.Fields = models.Fields{field: pp.Value}
		p.Tags = pp.Tags
		return p
	}
	p := models.Point{}
	p.Fields = models.Fields{field: v}
	p.Time = tmax
	return p
}

// take the result of a reduce operation and convert it to a batch
func reduceResultToBatch(field string, value interface{}, tmax time.Time, usePointTimes bool) models.Batch {
	b := models.Batch{}
	b.TMax = tmax
	switch v := value.(type) {
	case tsdb.PositionPoints:
		b.Points = make([]models.BatchPoint, len(v))
		for i, pp := range v {
			if usePointTimes {
				b.Points[i].Time = time.Unix(pp.Time, 0).UTC()
			} else {
				b.Points[i].Time = tmax
			}
			b.Points[i].Fields = models.Fields{field: pp.Value}
			b.Points[i].Fields[field] = pp.Value
			b.Points[i].Tags = pp.Tags
		}
	case tsdb.PositionPoint:
		b.Points = make([]models.BatchPoint, 1)
		if usePointTimes {
			b.Points[0].Time = time.Unix(v.Time, 0).UTC()
		} else {
			b.Points[0].Time = tmax
		}
		b.Points[0].Fields = models.Fields{field: v.Value}
		b.Points[0].Fields[field] = v.Value
		b.Points[0].Tags = v.Tags
	case tsdb.InterfaceValues:
		b.Points = make([]models.BatchPoint, len(v))
		for i, p := range v {
			b.Points[i].Time = tmax
			b.Points[i].Fields = models.Fields{field: p}
		}
	default:
		b.Points = make([]models.BatchPoint, 1)
		b.Points[0].Time = tmax
		b.Points[0].Fields = models.Fields{field: v}
	}
	return b
}
