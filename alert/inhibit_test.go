package alert_test

import (
	"testing"

	"github.com/influxdata/kapacitor/alert"
	"github.com/influxdata/kapacitor/models"
)

func TestInhibitor_IsInhibited(t *testing.T) {
	testCases := []struct {
		category  string
		inhibitor *alert.Inhibitor
		inhibited bool
		alertName string
		tags      models.Tags
		want      bool
	}{
		{
			category:  "exact match",
			inhibitor: alert.NewInhibitor("alert", models.Tags{"a": "x"}),
			inhibited: true,
			alertName: "alert",
			tags:      models.Tags{"a": "x"},
			want:      true,
		},
		{
			category:  "not inhibited",
			inhibitor: alert.NewInhibitor("alert", models.Tags{"a": "x"}),
			inhibited: false,
			alertName: "alert",
			tags:      models.Tags{"a": "x"},
			want:      false,
		},
		{
			category:  "not category match",
			inhibitor: alert.NewInhibitor("alert", models.Tags{"a": "x"}),
			inhibited: true,
			alertName: "foo",
			tags:      models.Tags{"a": "x"},
			want:      false,
		},
		{
			category:  "not tag match",
			inhibitor: alert.NewInhibitor("alert", models.Tags{"a": "x"}),
			inhibited: true,
			alertName: "alert",
			tags:      models.Tags{"a": "y"},
			want:      false,
		},
		{
			category:  "match with extra tags",
			inhibitor: alert.NewInhibitor("alert", models.Tags{"a": "x"}),
			inhibited: true,
			alertName: "alert",
			tags:      models.Tags{"a": "x", "b": "y"},
			want:      true,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.category, func(t *testing.T) {
			tc.inhibitor.Set(tc.inhibited)
			got := tc.inhibitor.IsInhibited(tc.alertName, tc.tags)
			if tc.want != got {
				t.Errorf("unexpected IsInhibited result got: %t want: %t", got, tc.want)
			}
		})
	}
}

func TestInhibitorLookup_IsInhibited(t *testing.T) {
	lookup := alert.NewInhibitorLookup()

	hostA := models.Tags{"host": "A"}
	hostB := models.Tags{"host": "B"}

	fooA := alert.NewInhibitor("foo", hostA)
	fooA.Set(true)
	fooB := alert.NewInhibitor("foo", hostB)
	fooB.Set(true)
	barA := alert.NewInhibitor("bar", hostA)
	barA.Set(true)
	barB := alert.NewInhibitor("bar", hostB)
	barB.Set(true)

	lookup.AddInhibitor(fooA)
	lookup.AddInhibitor(barA)
	lookup.AddInhibitor(barB)

	assert := func(got, want bool, msg string) {
		t.Helper()
		if want != got {
			t.Errorf("unexpected IsInhibited(%s) got: %t want %t", msg, got, want)
		}
	}

	assert(
		lookup.IsInhibited("foo", hostA),
		true,
		"foo host A pre-add fooB",
	)

	assert(
		lookup.IsInhibited("foo", hostB),
		false,
		"foo host B pre-add fooB",
	)

	assert(
		lookup.IsInhibited("bar", hostB),
		true,
		"bar host B pre-add fooB",
	)

	lookup.AddInhibitor(fooB)

	assert(
		lookup.IsInhibited("foo", hostA),
		true,
		"foo host A post-add fooB",
	)

	assert(
		lookup.IsInhibited("foo", hostB),
		true,
		"foo host B post-add fooB",
	)

	assert(
		lookup.IsInhibited("bar", hostB),
		true,
		"bar host B post-add fooB",
	)

	lookup.RemoveInhibitor(barB)

	assert(
		lookup.IsInhibited("foo", hostB),
		true,
		"foo host B post-remove barB",
	)

	assert(
		lookup.IsInhibited("foo", hostA),
		true,
		"foo host A post-remove barB",
	)

	assert(
		lookup.IsInhibited("bar", hostB),
		false,
		"bar host B post-remove barB",
	)
}
