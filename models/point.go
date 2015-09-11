package models

import (
	"bytes"
	"sort"
	"time"

	"github.com/influxdb/influxdb/models"
)

type GroupID string

const (
	NilGroup GroupID = ""
)

// Represents a single data point
type Point struct {
	Name string

	Group GroupID

	Tags map[string]string

	Fields map[string]interface{}

	Time time.Time
}

func NewPoint(
	name string,
	g GroupID,
	tags map[string]string,
	fields map[string]interface{},
	t time.Time,
) *Point {
	p := &Point{
		Name:   name,
		Group:  g,
		Tags:   tags,
		Fields: fields,
		Time:   t,
	}

	return p
}

// Returns byte array of a line protocol representation of the point
func (p *Point) Bytes(precision string) []byte {
	mp := models.NewPoint(
		p.Name,
		p.Tags,
		p.Fields,
		p.Time,
	)

	return []byte(mp.PrecisionString(precision))
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
