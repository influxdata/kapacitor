package pipeline

import (
	"fmt"
	"testing"
	"time"
)

func TestWindowNodeJSON(t *testing.T) {
	win := WindowNode{
		Period:         time.Hour,
		Every:          time.Minute,
		AlignFlag:      true,
		FillPeriodFlag: true,
		PeriodCount:    5,
		EveryCount:     5,
	}

	b, err := Marshal(&win)
	if err != nil {
		t.Fatalf("TestWindowNodeJSON() error marshaling json %v", err)
	}

	got := string(b)
	want := `{
    "period": 3600000000000,
    "every": 60000000000,
    "align": true,
    "fill_period": true,
    "period_count": 5,
    "every_count": 5,
    "typeOf": "window"
}`
	if got != want {
		t.Errorf("TestWindowNodeJSON() got\n%s\nwant\n%s", got, want)
		fmt.Println(got)
	}

}
