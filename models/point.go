package models

import (
	"bytes"

	"github.com/influxdata/kapacitor/istrings"
)

type GroupID istrings.IString

var (
	NilGroup GroupID
)

type Dimensions struct {
	ByName   bool
	TagNames []istrings.IString
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
	tags := make([]istrings.IString, len(d.TagNames))
	copy(tags, d.TagNames)
	return Dimensions{ByName: d.ByName, TagNames: tags}
}

func (d Dimensions) ToSet() map[istrings.IString]bool {
	set := make(map[istrings.IString]bool, len(d.TagNames))
	for _, dim := range d.TagNames {
		set[dim] = true
	}
	return set
}

type Fields map[istrings.IString]interface{}

func (f Fields) Copy() Fields {
	cf := make(Fields, len(f))
	for k, v := range f {
		cf[k] = v
	}
	return cf
}

// MarshalBinary encodes all the fields to their proper type and returns the binary
// represenation
// NOTE: uint64 is specifically not supported due to potential overflow when we decode
// again later to an int64
// NOTE2: uint is accepted, and may be 64 bits, and is for some reason accepted...
func (p Fields) MarshalBinary() []byte {
	var b []byte
	keys := make([]istrings.IString, 0, len(p))

	for k := range p {
		keys = append(keys, k)
	}

	// Not really necessary, can probably be removed.
	istrings.Sort(keys)

	for i, k := range keys {
		if i > 0 {
			b = append(b, ',')
		}
		b = appendField(b, k, p[k])
	}

	return b
}

func SortedFields(fields Fields) []istrings.IString {
	a := make([]istrings.IString, 0, len(fields))
	for k := range fields {
		a = append(a, k)
	}
	istrings.Sort(a)
	return a
}

type Tags map[istrings.IString]istrings.IString

func (t Tags) Copy() Tags {
	ct := make(Tags, len(t))
	for k, v := range t {
		ct[k] = v
	}
	return ct
}

type byteSlices [][]byte

func (a byteSlices) Len() int           { return len(a) }
func (a byteSlices) Less(i, j int) bool { return bytes.Compare(a[i], a[j]) == -1 }
func (a byteSlices) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

func SortedKeys(tags map[istrings.IString]istrings.IString) []istrings.IString {
	a := make([]istrings.IString, 0, len(tags))
	for k := range tags {
		a = append(a, k)
	}
	istrings.Sort(a)
	return a
}

func ToGroupID(name istrings.IString, tags Tags, dims Dimensions) GroupID {
	if len(dims.TagNames) == 0 {
		if dims.ByName {
			return GroupID(name)
		}
		return NilGroup
	}
	var buf istrings.Builder
	l := 0
	if dims.ByName {
		// add capacity for the name + "\n"
		l += name.Len() + 1
	}
	for i, d := range dims.TagNames {
		if i != 0 {
			// add capacity for the comma after the tagnames
			l++
		}
		// add capacity for the name length, and the tag length, and the "="
		l += d.Len() + tags[d].Len() + 1
	}
	buf.Grow(l)
	if dims.ByName {
		buf.WriteIString(name)
		// Add delimiter that is not allowed in name.
		buf.WriteRune('\n')
	}
	for i, d := range dims.TagNames {
		if i != 0 {
			buf.WriteRune(',')
		}
		buf.WriteIString(d)
		buf.WriteRune('=')
		buf.WriteIString(tags[d])
	}
	return GroupID(buf.IString())
}
