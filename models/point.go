package models

import (
	"bytes"
	"sort"
	"strconv"
	"time"

	"github.com/influxdata/influxdb/models"
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
	PointDimensions() Dimensions
	PointFields() Fields

	// Return a copy of self
	Copy() PointInterface
	Setter() PointSetter
}

type PointSetter interface {
	PointInterface
	SetNewDimTag(key string, value string)
	UpdateGroup()
	Interface() PointInterface
}

type Dimensions struct {
	ByName   bool
	TagNames []string
}

// Represents a single data point
type Point struct {
	Name            string
	Database        string
	RetentionPolicy string

	Group      GroupID
	Dimensions Dimensions

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
	tags := make(Tags, len(p.Dimensions.TagNames))
	for _, dim := range p.Dimensions.TagNames {
		tags[dim] = p.Tags[dim]
	}
	return tags
}

func (p Point) PointDimensions() Dimensions {
	return p.Dimensions
}

func (p Point) Copy() PointInterface {
	cp := p
	cp.Fields = p.Fields.Copy()
	cp.Tags = p.Tags.Copy()
	cp.Dimensions = p.Dimensions.Copy()
	return &cp
}

func (p Point) Setter() PointSetter {
	return &p
}

func (p *Point) Interface() PointInterface {
	return *p
}

func (p *Point) SetNewDimTag(key string, value string) {
	p.Tags[key] = value
	// Only add dim if it does not exist.
	for _, dim := range p.Dimensions.TagNames {
		if dim == key {
			// Key exists we are done.
			return
		}
	}
	// Key doesn't exist add it.
	p.Dimensions.TagNames = append(p.Dimensions.TagNames, key)
}

func (p *Point) UpdateGroup() {
	sort.Strings(p.Dimensions.TagNames)
	p.Group = ToGroupID(p.Name, p.Tags, p.Dimensions)
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

func ToGroupID(name string, tags map[string]string, dims Dimensions) GroupID {
	if len(dims.TagNames) == 0 {
		if dims.ByName {
			return GroupID(name)
		}
		return NilGroup
	}
	var buf bytes.Buffer
	if dims.ByName {
		buf.WriteString(name)
		// Add delimiter that is not allowed in name.
		buf.WriteRune('\n')
	}
	for i, d := range dims.TagNames {
		if i != 0 {
			buf.WriteRune(',')
		}
		buf.WriteString(d)
		buf.WriteRune('=')
		buf.WriteString(tags[d])

	}
	return GroupID(buf.Bytes())
}

// Returns byte array of a line protocol representation of the point
func (p Point) Bytes(precision string) []byte {
	key := models.MakeKey([]byte(p.Name), models.NewTags(p.Tags))
	fields := models.Fields(p.Fields).MarshalBinary()
	kl := len(key)
	fl := len(fields)
	var bytes []byte

	if p.Time.IsZero() {
		bytes = make([]byte, fl+kl+1)
		copy(bytes, key)
		bytes[kl] = ' '
		copy(bytes[kl+1:], fields)
	} else {
		timeStr := strconv.FormatInt(p.Time.UnixNano()/models.GetPrecisionMultiplier(precision), 10)
		tl := len(timeStr)
		bytes = make([]byte, fl+kl+tl+2)
		copy(bytes, key)
		bytes[kl] = ' '
		copy(bytes[kl+1:], fields)
		bytes[kl+fl+1] = ' '
		copy(bytes[kl+fl+2:], []byte(timeStr))
	}

	return bytes
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

func (t Tags) Copy() Tags {
	ct := make(Tags, len(t))
	for k, v := range t {
		ct[k] = v
	}
	return ct
}

func (d Dimensions) Copy() Dimensions {
	tags := make([]string, len(d.TagNames))
	copy(tags, d.TagNames)
	return Dimensions{ByName: d.ByName, TagNames: tags}
}

func (d Dimensions) ToSet() map[string]bool {
	set := make(map[string]bool, len(d.TagNames))
	for _, dim := range d.TagNames {
		set[dim] = true
	}
	return set
}

// Simple container for point data.
type RawPoint struct {
	Time   time.Time
	Fields Fields
	Tags   Tags
}
