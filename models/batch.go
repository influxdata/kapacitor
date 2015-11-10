package models

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/influxdb/influxdb/client"
	"github.com/influxdb/influxdb/models"
)

//Represents purely a set of fields and a time.
type BatchPoint struct {
	Fields Fields            `json:"fields"`
	Time   time.Time         `json:"time"`
	Tags   map[string]string `json:"tags,omitempty"`
}

type Batch struct {
	Name   string            `json:"name,omitempty"`
	Group  GroupID           `json:"-"`
	TMax   time.Time         `json:"-"`
	Tags   map[string]string `json:"tags,omitempty"`
	Points []BatchPoint      `json:"points,omitempty"`
}

func BatchPointFromPoint(p Point) BatchPoint {
	return BatchPoint{
		Time:   p.Time,
		Fields: p.Fields,
		Tags:   p.Tags,
	}
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
	for f := range p.Fields {
		row.Columns = append(row.Columns, f)
	}
	// Append tags that are not on the batch
	for t := range p.Tags {
		if _, ok := b.Tags[t]; !ok {
			row.Columns = append(row.Columns, t)
		}
	}
	// Sort all columns but leave time as first
	sort.Strings(row.Columns[1:])
	row.Values = make([][]interface{}, len(b.Points))
	for i, p := range b.Points {
		row.Values[i] = make([]interface{}, len(row.Columns))
		row.Values[i][0] = p.Time
		for j, c := range row.Columns[1:] {
			if v, ok := p.Fields[c]; ok {
				row.Values[i][j+1] = v
			} else if v, ok := p.Tags[c]; ok {
				row.Values[i][j+1] = v
			}
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
		b.Points = make([]BatchPoint, 0, len(series.Values))
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
					BatchPoint{Time: t, Fields: fields, Tags: b.Tags},
				)
			}
		}
		batches[i] = b
	}
	return batches, nil
}
