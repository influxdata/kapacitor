package kapacitor

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"time"
	"unicode"

	dbmodels "github.com/influxdb/influxdb/models"
	"github.com/influxdb/kapacitor/clock"
	"github.com/influxdb/kapacitor/models"
)

const (
	batchBegin = "BEGIN"
	batchEnd   = "END"
)

// Replay engine that can replay static data sets against a specific executor and its tasks.
type Replay struct {
	clck   clock.Clock
	Setter clock.Setter
}

// Create a new replay engine.
func NewReplay(c clock.Clock) *Replay {
	return &Replay{
		clck:   c,
		Setter: c,
	}
}

// Replay a data set against an executor.
func (r *Replay) ReplayStream(data io.ReadCloser, stream StreamCollector) <-chan error {
	src := newReplaySource(data, r.clck)
	src.ReplayStream(stream)
	return src.Err()
}

// Replay a data set against an executor.
func (r *Replay) ReplayBatch(data io.ReadCloser, batch BatchCollector) <-chan error {
	src := newReplaySource(data, r.clck)
	src.ReplayBatch(batch)
	return src.Err()
}

func newReplaySource(data io.ReadCloser, clck clock.Clock) *replaySource {
	return &replaySource{
		data: data,
		in:   bufio.NewScanner(data),
		clck: clck,
		err:  make(chan error, 1),
	}
}

type replaySource struct {
	data io.Closer
	in   *bufio.Scanner
	clck clock.Clock
	err  chan error
}

func (r *replaySource) ReplayStream(stream StreamCollector) {
	go func() {
		defer stream.Close()
		defer r.data.Close()
		start := time.Time{}
		var diff time.Duration
		zero := r.clck.Zero()
		for r.in.Scan() {
			db := r.in.Text()
			if !r.in.Scan() {
				r.err <- fmt.Errorf("invalid replay file format, expected another line")
				return
			}
			rp := r.in.Text()
			if !r.in.Scan() {
				r.err <- fmt.Errorf("invalid replay file format, expected another line")
				return
			}
			points, err := dbmodels.ParsePointsWithPrecision(
				r.in.Bytes(),
				zero,
				"s",
			)
			if err != nil {
				r.err <- err
				return
			}
			if start.IsZero() {
				start = points[0].Time()
				diff = zero.Sub(start)
			}
			t := points[0].Time().Add(diff).UTC()
			mp := points[0]
			p := models.Point{
				Database:        db,
				RetentionPolicy: rp,
				Name:            mp.Name(),
				Group:           models.NilGroup,
				Tags:            mp.Tags(),
				Fields:          models.Fields(mp.Fields()),
				Time:            t,
			}
			r.clck.Until(t)
			err = stream.CollectPoint(p)
			if err != nil {
				r.err <- err
				return
			}
		}
		r.err <- r.in.Err()
	}()
}

func (r *replaySource) ReplayBatch(batch BatchCollector) {
	go func() {
		defer batch.Close()
		defer r.data.Close()

		// Set SplitFunc
		r.in.Split(scanObj)

		// Find relative times
		start := time.Time{}
		var diff time.Duration
		zero := r.clck.Zero()

		for r.in.Scan() {
			var br batchResult
			err := json.Unmarshal(r.in.Bytes(), &br)
			if err != nil {
				r.err <- err
				return
			}

			if start.IsZero() {
				var err error
				start, err = time.Parse(time.RFC3339, br.Results[0].Series[0].Values[0][0].(string))
				if err != nil {
					r.err <- err
					return
				}
				diff = zero.Sub(start)
			}
			var lastTime time.Time
			for _, res := range br.Results {
				for _, series := range res.Series {
					b := models.Batch{
						Name: series.Name,
						Group: models.TagsToGroupID(
							models.SortedKeys(series.Tags),
							series.Tags,
						),
						Tags:   series.Tags,
						Points: make([]models.TimeFields, len(series.Values)),
					}
					for i, v := range series.Values {
						tf := models.TimeFields{}
						tf.Fields = make(models.Fields, len(series.Columns))
						for i, c := range series.Columns {
							if c == "time" {
								st, err := time.Parse(time.RFC3339, v[i].(string))
								if err != nil {
									r.err <- err
									return
								}
								tf.Time = st.Add(diff).UTC()
							} else {
								tf.Fields[c] = v[i]
							}
						}
						lastTime = tf.Time
						b.Points[i] = tf
					}
					r.clck.Until(lastTime)
					batch.CollectBatch(b)
				}
			}
		}
		r.err <- r.in.Err()
	}()
}

type batchResult struct {
	Results []struct {
		Series []struct {
			Name    string
			Tags    map[string]string
			Columns []string
			Values  [][]interface{}
		}
	}
}

// Find '{' '}' pairs and return the token
func scanObj(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}

	// scan past any whitespace
	start := 0
	for ; start < len(data) && unicode.IsSpace(rune(data[start])); start++ {
	}

	if start == len(data) {
		return 0, nil, nil
	}

	if !atEOF && data[start] != '{' {
		return 0, nil, fmt.Errorf("unexpected token %q, expected '{'", data[start])
	}
	depth := 0
	for i, b := range data[start:] {
		if b == '{' {
			depth++
		} else if b == '}' {
			depth--
		}
		if depth == 0 {
			l := start + i + 1
			return l, data[start:l], nil
		}
	}
	if atEOF {
		return 0, nil, fmt.Errorf("unterminated object")
	}
	return 0, nil, nil
}

func (r *replaySource) Err() <-chan error {
	return r.err
}
