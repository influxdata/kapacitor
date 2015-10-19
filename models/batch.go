package models

import (
	"time"

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
