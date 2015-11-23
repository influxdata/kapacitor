package models

import (
	"bytes"
	"sort"
	"time"

	"github.com/influxdb/influxdb/models"
)

type GroupID string
type Fields map[string]interface{}
type Tags map[string]string

const (
	NilGroup GroupID = ""
)

// Common interface for both Point and Batch objects
type PointInterface interface {
	PointName() string
	PointTime() time.Time
	PointGroup() GroupID
	PointTags() Tags
	PointDimensions() []string
	PointFields() Fields
}

// Represents a single data point
type Point struct {
	Name            string
	Database        string
	RetentionPolicy string

	Group      GroupID
	Dimensions []string

	Tags Tags

	Fields Fields

	Time time.Time
}

func (p Point) PointName() string {
	return p.Name
}

func (p Point) PointGroup() GroupID {
	return p.Group
}

func (p Point) PointTime() time.Time {
	return p.Time
}

func (p Point) PointFields() Fields {
	return p.Fields
}

func (p Point) PointTags() Tags {
	tags := make(Tags, len(p.Dimensions))
	for _, dim := range p.Dimensions {
		tags[dim] = p.Tags[dim]
	}
	return tags
}

func (p Point) PointDimensions() []string {
	return p.Dimensions
}

// Returns byte array of a line protocol representation of the point
func (p Point) Bytes(precision string) []byte {
	mp, err := models.NewPoint(
		p.Name,
		map[string]string(p.Tags),
		map[string]interface{}(p.Fields),
		p.Time,
	)
	if err != nil {
		panic(err)
	}

	return []byte(mp.PrecisionString(precision))
}

func SortedFields(fields Fields) []string {
	a := make([]string, 0, len(fields))
	for k := range fields {
		a = append(a, k)
	}
	sort.Strings(a)
	return a
}

func SortedKeys(tags map[string]string) []string {
	a := make([]string, 0, len(tags))
	for k := range tags {
		a = append(a, k)
	}
	sort.Strings(a)
	return a
}

func TagsToGroupID(dims []string, tags map[string]string) GroupID {
	var buf bytes.Buffer
	for _, d := range dims {
		buf.Write([]byte(d))
		buf.Write([]byte("="))
		buf.Write([]byte(tags[d]))
		buf.Write([]byte(","))
	}

	return GroupID(buf.Bytes())
}

func PointToRow(p Point) (row *models.Row) {
	row = &models.Row{
		Name:    p.Name,
		Tags:    p.Tags,
		Columns: []string{"time"},
		Values:  make([][]interface{}, 1),
	}

	for _, f := range SortedFields(p.Fields) {
		row.Columns = append(row.Columns, f)
	}
	row.Values[0] = make([]interface{}, len(p.Fields)+1)
	row.Values[0][0] = p.Time
	for i, c := range row.Columns[1:] {
		row.Values[0][i+1] = p.Fields[c]
	}
	return
}

func (f Fields) Copy() Fields {
	cf := make(Fields, len(f))
	for k, v := range f {
		cf[k] = v
	}
	return cf
}
