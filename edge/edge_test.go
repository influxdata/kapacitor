package edge_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
)

const defaultEdgeBufferSize = 1000

var name = "edge_test"
var db = "mydb"
var rp = "myrp"
var now = time.Now()
var groupTags = models.Tags{
	"tag1": "value1",
	"tag2": "value2",
}
var groupDims = models.Dimensions{TagNames: []string{"tag1", "tag2"}}

var point = edge.NewPointMessage(
	name,
	db,
	rp,
	groupDims,
	models.Fields{
		"field1": 42,
		"field2": 4.2,
		"field3": 49,
		"field4": 4.9,
	},
	models.Tags{
		"tag1": "value1",
		"tag2": "value2",
		"tag3": "value3",
		"tag4": "value4",
	},
	now,
)

var batch = edge.NewBufferedBatchMessage(
	edge.NewBeginBatchMessage(
		name,
		groupTags,
		groupDims.ByName,
		now,
		2,
	),
	[]edge.BatchPointMessage{
		edge.NewBatchPointMessage(
			models.Fields{
				"field1": 42,
				"field2": 4.2,
				"field3": 49,
				"field4": 4.9,
			},
			models.Tags{
				"tag1": "value1",
				"tag2": "value2",
				"tag3": "first",
				"tag4": "first",
			},
			now,
		),
		edge.NewBatchPointMessage(
			models.Fields{
				"field1": 42,
				"field2": 4.2,
				"field3": 49,
				"field4": 4.9,
			},
			models.Tags{
				"tag1": "value1",
				"tag2": "value2",
				"tag3": "second",
				"tag4": "second",
			},
			now,
		),
	},
	edge.NewEndBatchMessage(),
)

func TestEdge_CollectPoint(t *testing.T) {
	e := edge.NewChannelEdge(pipeline.StreamEdge, defaultEdgeBufferSize)

	e.Collect(point)
	msg, ok := e.Emit()
	if !ok {
		t.Fatal("did not get point back out of edge")
	}
	if !reflect.DeepEqual(msg, point) {
		t.Errorf("unexpected point after passing through edge:\ngot:\n%v\nexp:\n%v\n", msg, point)
	}
}

func TestEdge_CollectBatch(t *testing.T) {
	e := edge.NewChannelEdge(pipeline.BatchEdge, defaultEdgeBufferSize)
	e.Collect(batch)
	msg, ok := e.Emit()
	if !ok {
		t.Fatal("did not get batch back out of edge")
	}
	if !reflect.DeepEqual(batch, msg) {
		t.Errorf("unexpected batch after passing through edge:\ngot:\n%v\nexp:\n%v\n", msg, batch)
	}
}

var emittedMsg edge.Message
var emittedOK bool

func BenchmarkCollectPoint(b *testing.B) {
	e := edge.NewChannelEdge(pipeline.StreamEdge, defaultEdgeBufferSize)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			e.Collect(point)
			emittedMsg, emittedOK = e.Emit()
		}
	})
}

func BenchmarkCollectBatch(b *testing.B) {
	e := edge.NewChannelEdge(pipeline.StreamEdge, defaultEdgeBufferSize)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			e.Collect(batch)
			emittedMsg, emittedOK = e.Emit()
		}
	})
}

type noopReceiver struct{}

func (r noopReceiver) BeginBatch(begin edge.BeginBatchMessage) error {
	return nil
}

func (r noopReceiver) BatchPoint(bp edge.BatchPointMessage) error {
	return nil
}

func (r noopReceiver) EndBatch(end edge.EndBatchMessage) error {
	return nil
}

func (r noopReceiver) Point(p edge.PointMessage) error {
	return nil
}

func (r noopReceiver) Barrier(b edge.BarrierMessage) error {
	return nil
}
func (r noopReceiver) DeleteGroup(d edge.DeleteGroupMessage) error {
	return nil
}
func (r noopReceiver) Done() {
}

func BenchmarkConsumer(b *testing.B) {
	var msg edge.Message
	msg = batch
	count := defaultEdgeBufferSize * 10
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		e := edge.NewChannelEdge(pipeline.StreamEdge, defaultEdgeBufferSize)
		consumer := edge.NewConsumerWithReceiver(e, noopReceiver{})
		go func() {
			for i := 0; i < count; i++ {
				e.Collect(msg)
			}
			e.Close()
		}()
		b.StartTimer()
		err := consumer.Consume()
		if err != nil {
			b.Fatal(err)
		}
	}
}
