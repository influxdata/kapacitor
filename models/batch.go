package models

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/kapacitor/influxdb"
)

// A point in batch, similar to Point but most information is
// found on the containing Batch.
//
// Tags on a BatchPoint are a superset of the tags on the Batch
// All points in a batch should have the same tag and field keys.
type BatchPoint struct {
	Time   time.Time `json:"time"`
	Fields Fields    `json:"fields"`
	Tags   Tags      `json:"tags"`
}

func BatchPointFromPoint(p Point) BatchPoint {
	return BatchPoint{
		Time:   p.Time,
		Fields: p.Fields,
		Tags:   p.Tags,
	}
}

type Batch struct {
	Name   string       `json:"name,omitempty"`
	TMax   time.Time    `json:"tmax,omitempty"`
	Group  GroupID      `json:"group,omitempty"`
	ByName bool         `json:"byname,omitempty"`
	Tags   Tags         `json:"tags,omitempty"`
	Points []BatchPoint `json:"points,omitempty"`
}

func (b Batch) PointName() string {
	return b.Name
}
func (b Batch) PointGroup() GroupID {
	return b.Group
}
func (b Batch) PointTime() time.Time {
	return b.TMax
}

func (b Batch) PointFields() Fields {
	if len(b.Points) > 0 {
		return b.Points[0].Fields
	}
	return nil
}

func (b Batch) PointTags() Tags {
	return b.Tags
}

func (b Batch) PointDimensions() Dimensions {
	return Dimensions{
		ByName:   b.ByName,
		TagNames: SortedKeys(b.Tags),
	}
}

func (b Batch) Copy() PointInterface {
	cb := b
	cb.Tags = b.Tags.Copy()
	cb.Points = make([]BatchPoint, len(b.Points))
	for i, p := range b.Points {
		cb.Points[i] = p
		cb.Points[i].Fields = p.Fields.Copy()
		cb.Points[i].Tags = p.Tags.Copy()
	}
	return cb
}

func (b Batch) Setter() PointSetter {
	return &b
}

func (b *Batch) Interface() PointInterface {
	return *b
}

func (b *Batch) SetNewDimTag(key string, value string) {
	b.Tags[key] = value
	for _, p := range b.Points {
		p.Tags[key] = value
	}
}

func (b *Batch) UpdateGroup() {
	b.Group = ToGroupID(b.Name, b.Tags, b.PointDimensions())
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

func ResultToBatches(res influxdb.Result, groupByName bool) ([]Batch, error) {
	if res.Err != "" {
		return nil, errors.New(res.Err)
	}
	batches := make([]Batch, 0, len(res.Series))
	dims := Dimensions{
		ByName: groupByName,
	}
	for _, series := range res.Series {
		var name string
		if groupByName {
			name = series.Name
		}
		dims.TagNames = SortedKeys(series.Tags)
		groupID := ToGroupID(
			name,
			series.Tags,
			dims,
		)
		b := Batch{
			Name:  series.Name,
			Group: groupID,
			Tags:  series.Tags,
		}
		b.Points = make([]BatchPoint, 0, len(series.Values))
		for _, v := range series.Values {
			fields := make(Fields)
			var t time.Time
			for i, c := range series.Columns {
				if c == "time" {
					tStr, ok := v[i].(string)
					if !ok {
						return nil, fmt.Errorf("unexpected time value: %v", v[i])
					}
					var err error
					t, err = time.Parse(time.RFC3339Nano, tStr)
					if err != nil {
						t, err = time.Parse(time.RFC3339, tStr)
						if err != nil {
							return nil, fmt.Errorf("unexpected time format: %v", err)
						}
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
						break
					}
					fields[c] = value
				}
			}
			if len(fields) > 0 {
				if t.After(b.TMax) {
					b.TMax = t
				}
				b.Points = append(
					b.Points,
					BatchPoint{Time: t, Fields: fields, Tags: b.Tags},
				)
			}
		}
		batches = append(batches, b)
	}
	return batches, nil
}
