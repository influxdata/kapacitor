package kapacitor_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/influxdata/kapacitor"
)

func TestQuery_Clone(t *testing.T) {
	testCases := []string{
		"SELECT usage FROM telegraf.autogen.cpu",
		"SELECT mean(usage) FROM telegraf.autogen.cpu WHERE host = 'serverA'",
		"SELECT mean(usage) FROM telegraf.autogen.cpu WHERE host = 'serverA' AND dc = 'slc'",
		"SELECT mean(usage) FROM telegraf.autogen.cpu WHERE host = 'serverA' AND dc = 'slc' OR product = 'login'",
		"SELECT mean(usage) FROM telegraf.autogen.cpu WHERE host = 'serverA' AND (dc = 'slc' OR product = 'login')",
	}

	equal := func(q0, q1 *kapacitor.Query) error {
		if got, exp := q0.String(), q1.String(); got != exp {
			return fmt.Errorf("unequal query string: got %s exp %s", got, exp)
		}
		if got, exp := q0.StartTime(), q1.StartTime(); got != exp {
			return fmt.Errorf("unequal query start time: got %v exp %v", got, exp)
		}
		if got, exp := q0.StopTime(), q1.StopTime(); got != exp {
			return fmt.Errorf("unequal query stop time: got %v exp %v", got, exp)
		}
		if got, exp := q0.IsGroupedByTime(), q1.IsGroupedByTime(); got != exp {
			return fmt.Errorf("unequal query IsGroupedByTime: got %v exp %v", got, exp)
		}
		return nil
	}
	for _, query := range testCases {
		q, err := kapacitor.NewQuery(query)
		if err != nil {
			t.Fatal(err)
		}
		clone, err := q.Clone()
		if err != nil {
			t.Fatal(err)
		}
		if err := equal(clone, q); err != nil {
			t.Error(err)
		}

		// Modify original start time
		start := time.Date(1975, 1, 1, 0, 0, 0, 0, time.UTC)
		q.SetStartTime(start)

		if err := equal(clone, q); err == nil {
			t.Errorf("equal after modification: got %v", clone)
		}

		// Modify clone in the same way
		clone.SetStartTime(start)
		if err := equal(clone, q); err != nil {
			t.Error(err)
		}

		// Re-clone
		clone, err = q.Clone()
		if err != nil {
			t.Fatal(err)
		}
		if err := equal(clone, q); err != nil {
			t.Error(err)
		}

		// Modify original stop time
		stop := time.Date(1975, 1, 2, 0, 0, 0, 0, time.UTC)
		q.SetStopTime(stop)

		if err := equal(clone, q); err == nil {
			t.Errorf("equal after modification: got %v", clone)
		}

		// Modify clone in the same way
		clone.SetStopTime(stop)
		if err := equal(clone, q); err != nil {
			t.Error(err)
		}

		// Re-clone
		clone, err = q.Clone()
		if err != nil {
			t.Fatal(err)
		}
		if err := equal(clone, q); err != nil {
			t.Error(err)
		}

		// Set dimensions
		q.Dimensions([]interface{}{time.Hour})
		if err := equal(clone, q); err == nil {
			t.Errorf("equal after modification: got %v", clone)
		}
		// Set dimesions on the clone in the same way
		clone.Dimensions([]interface{}{time.Hour})
		if err := equal(clone, q); err != nil {
			t.Error(err)
		}
		// Re-clone
		clone, err = q.Clone()
		if err != nil {
			t.Fatal(err)
		}
		if err := equal(clone, q); err != nil {
			t.Error(err)
		}

		// Set group align and dimensions
		q.AlignGroup()
		q.Dimensions([]interface{}{kapacitor.TimeDimension{
			Length: time.Minute,
			Offset: time.Second,
		}})
		if err := equal(clone, q); err == nil {
			t.Errorf("equal after modification: got %v", clone)
			return
		}
		// Set group align and dimesions on the clone in the same way
		clone.AlignGroup()
		clone.Dimensions([]interface{}{kapacitor.TimeDimension{
			Length: time.Minute,
			Offset: time.Second,
		}})
		if err := equal(clone, q); err != nil {
			t.Error(err)
		}
		// Re-clone
		clone, err = q.Clone()
		if err != nil {
			t.Fatal(err)
		}
		if err := equal(clone, q); err != nil {
			t.Error(err)
		}
	}
}
func TestQuery_IsGroupedByTime(t *testing.T) {
	q, err := kapacitor.NewQuery("SELECT usage FROM telegraf.autogen.cpu")
	if err != nil {
		t.Fatal(err)
	}

	q.Dimensions([]interface{}{time.Hour})
	if !q.IsGroupedByTime() {
		t.Error("expected query to be grouped by time")
	}

	q, err = kapacitor.NewQuery("SELECT usage FROM telegraf.autogen.cpu")
	if err != nil {
		t.Fatal(err)
	}

	q.Dimensions([]interface{}{kapacitor.TimeDimension{Length: time.Hour, Offset: time.Minute}})
	if !q.IsGroupedByTime() {
		t.Error("expected query to be grouped by time")
	}

	q.Dimensions([]interface{}{"host"})
	if q.IsGroupedByTime() {
		t.Error("expected query to not be grouped by time")
	}
}
