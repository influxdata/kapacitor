package alert_test

import (
	"fmt"
	"github.com/influxdata/kapacitor/services/alert"
	"github.com/influxdata/kapacitor/services/alert/alerttest"
	"testing"
)

func BenchmarkTopicState_MarshalBinary(b *testing.B) {
	benchmarks := []struct {
		n int
	}{
		{n: 1},
		{n: 100},
		{n: 30000},
	}
	for _, bm := range benchmarks {
		b.Run(fmt.Sprintf("%d event state(s)", bm.n), func(b *testing.B) {

			var ts alert.TopicState
			ts.Topic = "topics/test/default"
			ts.EventStates = alerttest.MakeEventStates(alerttest.EventStateSpec{N: bm.n, Mwc: 5, Dwc: 15})

			totalMarshalBytes := int64(0)
			for k, _ := range ts.EventStates {
				data, err := ts.EventStates[k].MarshalJSON()
				if err != nil {
					panic(err)
				}
				totalMarshalBytes += int64(len(data))
			}
			b.SetBytes(totalMarshalBytes)

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				for k, _ := range ts.EventStates {
					_, err := ts.EventStates[k].MarshalJSON()
					if err != nil {
						panic(err)
					}
				}
			}
		})
	}
}

func BenchmarkTopicState_UnmarshalBinary(b *testing.B) {
	benchmarks := []struct {
		n int
	}{
		{n: 1},
		{n: 100},
		{n: 30000},
	}
	for _, bm := range benchmarks {
		b.Run(fmt.Sprintf("%d event state(s)", bm.n), func(b *testing.B) {

			var ts alert.TopicState
			ts.Topic = "topics/test/default"
			ts.EventStates = alerttest.MakeEventStates(alerttest.EventStateSpec{N: bm.n, Mwc: 5, Dwc: 15})

			marshaled := make([][]byte, 0, len(ts.EventStates))
			totalMarshalBytes := int64(0)
			for k, _ := range ts.EventStates {
				data, err := ts.EventStates[k].MarshalJSON()
				if err != nil {
					panic(err)
				}
				totalMarshalBytes += int64(len(data))
				marshaled = append(marshaled, data)
			}

			b.SetBytes(totalMarshalBytes)
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				for _, d := range marshaled {
					e := alert.EventState{}
					err := e.UnmarshalJSON(d)
					if err != nil {
						panic(err)
					}
				}
			}
		})
	}
}
