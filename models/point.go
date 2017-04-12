package models

import (
	"bytes"
	"sort"
)

type GroupID string

const (
	NilGroup GroupID = ""
)

type Dimensions struct {
	ByName   bool
	TagNames []string
}

func (d Dimensions) Equal(o Dimensions) bool {
	if d.ByName != o.ByName || len(d.TagNames) != len(o.TagNames) {
		return false
	}
	for i := range d.TagNames {
		if d.TagNames[i] != o.TagNames[i] {
			return false
		}
	}
	return true
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

type Fields map[string]interface{}

func (f Fields) Copy() Fields {
	cf := make(Fields, len(f))
	for k, v := range f {
		cf[k] = v
	}
	return cf
}

func SortedFields(fields Fields) []string {
	a := make([]string, 0, len(fields))
	for k := range fields {
		a = append(a, k)
	}
	sort.Strings(a)
	return a
}

type Tags map[string]string

func (t Tags) Copy() Tags {
	ct := make(Tags, len(t))
	for k, v := range t {
		ct[k] = v
	}
	return ct
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
