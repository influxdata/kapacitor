package kapacitor

import (
	"testing"
	"time"

	"github.com/influxdata/kapacitor/alert"
	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/keyvalue"
	"github.com/influxdata/kapacitor/models"
	"github.com/stretchr/testify/assert"
)

// Mock node diagnostic
type windowNodeDiagnostic struct{}

func newWindowNodeDiagnostic() *nodeDiagnostic { return &nodeDiagnostic{} }

func (d *windowNodeDiagnostic) Error(msg string, err error, ctx ...keyvalue.T) {}
func (d *windowNodeDiagnostic) AlertTriggered(level alert.Level, id string, message string, rows *models.Row) {
}
func (d *windowNodeDiagnostic) SettingReplicas(new int, old int, id string)                        {}
func (d *windowNodeDiagnostic) StartingBatchQuery(q string)                                        {}
func (d *windowNodeDiagnostic) LogBatchData(level, prefix string, batch edge.BufferedBatchMessage) {}
func (d *windowNodeDiagnostic) LogPointData(level, prefix string, point edge.PointMessage)         {}
func (d *windowNodeDiagnostic) UDFLog(s string)                                                    {}

func TestWindowBufferByTime(t *testing.T) {
	assert := assert.New(t)

	buf := &windowTimeBuffer{}

	size := 100

	// fill buffer
	for i := 1; i <= size; i++ {

		t := time.Unix(int64(i), 0)
		p := edge.NewPointMessage(
			"name", "db", "rp",
			models.Dimensions{},
			nil,
			nil,
			t,
		)
		buf.insert(p)

		assert.Equal(i, buf.size)
		assert.Equal(0, buf.start)
		assert.Equal(i, buf.stop)
	}

	// purge entire buffer
	for i := 0; i <= size; i++ {

		oldest := time.Unix(int64(i+1), 0).UTC()
		buf.purge(oldest, true)

		assert.Equal(size-i, buf.size, "i: %d", i)
		assert.Equal(i, buf.start, "i: %d", i)
		assert.Equal(size, buf.stop, "i: %d", i)

		points := buf.points()
		if assert.Equal(size-i, len(points)) {
			for _, p := range points {
				assert.True(!p.Time().Before(oldest), "Point %s is not after oldest time %s", p.Time(), oldest)
			}
		}
	}

	assert.Equal(0, buf.size)

	// fill buffer again
	oldest := time.Unix(int64(size), 0).UTC()
	for i := 1; i <= size*2; i++ {

		t := time.Unix(int64(i+size), 0)
		p := edge.NewPointMessage(
			"name", "db", "rp",
			models.Dimensions{},
			nil,
			nil,
			t,
		)
		buf.insert(p)

		assert.Equal(i, buf.size)

		points := buf.points()
		if assert.Equal(i, len(points)) {
			for _, p := range points {
				if assert.NotNil(p, "i:%d", i) {
					assert.True(!p.Time().Before(oldest), "Point %s is not after oldest time %s", p.Time(), oldest)
				}
			}
		}
	}
}

func TestWindowBufferByCount(t *testing.T) {
	testCases := []struct {
		size       int
		every      int
		period     int
		fillPeriod bool
	}{
		{
			size:   100,
			every:  10,
			period: 10,
		},
		{
			size:   100,
			every:  3,
			period: 10,
		},
		{
			size:   100,
			every:  1,
			period: 2,
		},
		{
			size:   100,
			every:  1,
			period: 1,
		},
		{
			size:   100,
			every:  10,
			period: 5,
		},
		{
			size:   100,
			every:  1,
			period: 5,
		},
	}
	for _, tc := range testCases {
		t.Logf("Starting test size %d period %d every %d", tc.size, tc.period, tc.every)
		w := newWindowByCount(
			"test",
			edge.GroupInfo{},
			tc.period,
			tc.every,
			tc.fillPeriod,
			newWindowNodeDiagnostic(),
		)

		// fill buffer
		for i := 1; i <= tc.size; i++ {
			p := edge.NewPointMessage(
				"name", "db", "rp",
				models.Dimensions{},
				nil,
				nil,
				time.Unix(int64(i), 0).UTC(),
			)
			msg, err := w.Point(p)
			if err != nil {
				t.Fatal(err)
			}
			expEmit := tc.every == 0 || i%tc.every == 0
			if tc.fillPeriod {
				expEmit = i > tc.period && expEmit
			}
			if expEmit && msg == nil {
				t.Errorf("%d unexpected nil forward message: got nil message, expected non nil message", i)
			}
			if !expEmit && msg != nil {
				t.Errorf("%d unexpected forward message: got non-nil message %v, expected nil message", i, msg)
			}

			size := i
			if size > tc.period {
				size = tc.period
			}
			if got, exp := w.size, size; got != exp {
				t.Errorf("%d unexpected size: got %d exp %d", i, got, exp)
			}
			start := (i - tc.period) % tc.period
			if start < 0 {
				start = 0
			}
			if got, exp := w.start, start; got != exp {
				t.Errorf("%d unexpected start: got %d exp %d", i, got, exp)
			}
			if got, exp := w.stop, i%tc.period; got != exp {
				t.Errorf("%d unexpected stop: got %d exp %d", i, got, exp)
			}

			if msg != nil {
				if msg.Type() != edge.BufferedBatch {
					t.Fatalf("unexpected message type %v", msg.Type())
				}
				b := msg.(edge.BufferedBatchMessage)
				l := i
				if l > tc.period {
					l = tc.period
				}
				points := b.Points()
				if got, exp := len(points), l; got != exp {
					t.Fatalf("%d unexpected number of points got %d exp %d", i, got, exp)
				}

				for j, p := range points {
					if got, exp := p.Time(), time.Unix(int64(i+j-len(points)+1), 0).UTC(); !got.Equal(exp) {
						t.Errorf("%d unexpected point[%d].Time: got %v exp %v", i, j, got, exp)
					}
				}
			}
		}
	}
}
