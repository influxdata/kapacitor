package models

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/influxdb/influxdb/client"
	"github.com/influxdb/influxdb/models"
)

//Represents purely a set of fields and a time.
type TimeFields struct {
	Fields Fields    `json:"fields"`
	Time   time.Time `json:"time"`
}

type Batch struct {
	Name   string            `json:"name,omitempty"`
	Group  GroupID           `json:"-"`
	Tags   map[string]string `json:"tags,omitempty"`
	Points []TimeFields      `json:"points,omitempty"`
}

func BatchToRow(b Batch) (row *models.Row) {
	row = &models.Row{
		Name: b.Name,
		Tags: b.Tags,
	}
	if len(b.Points) == 0 {
		return
	}
	row.Columns = []string{"time"}
	p := b.Points[0]
	for _, f := range SortedFields(p.Fields) {
		row.Columns = append(row.Columns, f)
	}
	row.Values = make([][]interface{}, len(b.Points))
	for i, p := range b.Points {
		row.Values[i] = make([]interface{}, len(p.Fields)+1)
		row.Values[i][0] = p.Time
		for j, c := range row.Columns[1:] {
			row.Values[i][j+1] = p.Fields[c]
		}
	}
	return
}

func ResultToBatches(res client.Result) ([]Batch, error) {
	if res.Err != nil {
		return nil, res.Err
	}
	batches := make([]Batch, len(res.Series))
	for i, series := range res.Series {
		groupID := TagsToGroupID(
			SortedKeys(series.Tags),
			series.Tags,
		)
		b := Batch{
			Name:  series.Name,
			Group: groupID,
			Tags:  series.Tags,
		}
		b.Points = make([]TimeFields, 0, len(series.Values))
		for _, v := range series.Values {
			var skip bool
			fields := make(Fields)
			var t time.Time
			for i, c := range series.Columns {
				if c == "time" {
					tStr, ok := v[i].(string)
					if !ok {
						return nil, fmt.Errorf("unexpected time value: %v", v[i])
					}
					var err error
					t, err = time.Parse(time.RFC3339, tStr)
					if err != nil {
						return nil, fmt.Errorf("unexpected time format: %v", err)
					}
				} else {
					value := v[i]
					if n, ok := value.(json.Number); ok {
						f, err := n.Float64()
						if err == nil {
							value = f
						}
					}
					if value == nil {
						skip = true
						break
					}
					fields[c] = value
				}
			}
			if !skip {
				b.Points = append(
					b.Points,
					TimeFields{Time: t, Fields: fields},
				)
			}
		}
		batches[i] = b
	}
	return batches, nil
}
