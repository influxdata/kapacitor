package kapacitor

import (
	"log"
	"os"
	"testing"

	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/wlog"
)

func BenchmarkCollectPoint(b *testing.B) {
	name := "point"
	b.ReportAllocs()
	ls := &logService{}
	e := newEdge("BCollectPoint", "parent", "child", pipeline.StreamEdge, defaultEdgeBufferSize, ls)
	p := models.Point{
		Name: name,
		Tags: models.Tags{
			"tag1": "value1",
			"tag2": "value2",
			"tag3": "value3",
			"tag4": "value4",
		},
		Group: models.ToGroupID(name, nil, models.Dimensions{}),
		Fields: models.Fields{
			"field1": 42,
			"field2": 4.2,
			"field3": 49,
			"field4": 4.9,
		},
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			e.CollectPoint(p)
			e.NextPoint()
		}
	})
}

type logService struct{}

func (l *logService) NewLogger(prefix string, flag int) *log.Logger {
	return wlog.New(os.Stderr, prefix, flag)
}
