package influxdb

import (
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/influxdata/kapacitor/istrings"

	"github.com/pkg/errors"
)

func NewFluxQueryResponse(r io.Reader) (*Response, error) {
	builder := responseBuilder{}
	err := NewFluxCSVEventParser(r, &builder).Parse()
	if err != nil {
		return nil, err
	}
	if builder.Err != nil {
		return nil, builder.Err
	}
	return &Response{Results: []Result{{Series: builder.buf}}}, nil
}

// queryCSVResult is the result of a flux query in CSV format
// it assumes a csv dialect with
// Annotations: []string{"datatype", "group"},
// Delimiter:   ",",
// Header:      true,
type responseBuilder struct {
	colNames       []istrings.IString
	colNamesMap    map[istrings.IString]int
	tags           []int
	measurementCol int
	fields         []int
	defaultVals    []istrings.IString
	Err            error
	buf            []Row
	seriesBuf      *Row
}

var (
	measurementIString = istrings.Get("_measurement")
	fluxTimeIString    = istrings.Get("_time")
	influxQLTimeString = istrings.Get("time")
	stopIString        = istrings.Get("_stop")
	startIString       = istrings.Get("_start")
)

func (q *responseBuilder) TableStart(meta FluxTableMetaData, firstRow []istrings.IString) {
	if q.Err != nil {
		return
	}
	q.seriesBuf = &Row{}
	tags := make(map[istrings.IString]istrings.IString, len(firstRow))
	// add the tags from the row
	for _, i := range q.tags {
		tags[q.colNames[i]] = firstRow[i]
	}
	// add the column names from the row
	for _, i := range q.fields {
		q.seriesBuf.Columns = append(q.seriesBuf.Columns, q.colNames[i])
	}
	q.seriesBuf.Tags = tags
	if i, ok := q.colNamesMap[measurementIString]; ok {
		q.seriesBuf.Name = firstRow[i]
	}
}

func (q *responseBuilder) TableEnd() {
	if q.Err != nil {
		return
	}
	if q.seriesBuf != nil {
		q.buf = append(q.buf, *q.seriesBuf)
	}
	q.seriesBuf = nil
}

func (q *responseBuilder) Error(err string) {
	if q.Err != nil {
		return
	}
	q.Err = errors.New("flux query error: " + err)
}

func (q *responseBuilder) GroupStart(names []istrings.IString, types []istrings.IString, groups []bool) {
	if q.Err != nil {
		return
	}
	q.colNames = q.colNames[:0]
	// replace "_time" that flux uses with "time"
	for i := range names {
		if names[i] == fluxTimeIString {
			q.colNames = append(q.colNames, influxQLTimeString)
		} else {
			q.colNames = append(q.colNames, names[i])
		}
	}
	q.colNamesMap = make(map[istrings.IString]int, len(q.colNames))
	q.tags = q.tags[:0]
	q.fields = q.fields[:0]
	for i := range q.colNames {
		cn := q.colNames[i]
		q.colNamesMap[cn] = i
		switch cn {
		case measurementIString, startIString, stopIString:
		default: // its a tag or a field
			if !groups[i] { // its a fields
				q.fields = append(q.fields, i)
			} else {
				q.tags = append(q.tags, i)
			}
		}
	}
}

func (q *responseBuilder) DataRow(meta FluxTableMetaData, row []istrings.IString) {
	if q.Err != nil {
		return
	}
	// Add the field values for the row
	values := make([]interface{}, 0, len(q.fields))
	for _, i := range q.fields {
		var err error
		val, err := q.convert(meta.DataTypes[i], row[i])
		if err != nil {
			q.Err = err
			return
		}
		values = append(values, val)
	}
	q.seriesBuf.Values = append(q.seriesBuf.Values, values)
}

// if it is part of the group key and not a time value named either _time, _start, or _stop
var (
	stringDatatype      = istrings.Get("string")
	timeDatatype        = istrings.Get("dateTime")
	floatDatatype       = istrings.Get("double")
	boolDatatype        = istrings.Get("boolean")
	intDatatype         = istrings.Get("long")
	uintDatatype        = istrings.Get("unsignedLong")
	timeDataTypeWithFmt = istrings.Get("dateTime:RFC3339")

	falseString = istrings.Get("false")
)

func (q *responseBuilder) convert(dataType, value istrings.IString) (interface{}, error) {
	s := value
	switch dataType {
	case stringDatatype:
		return s, nil
	case timeDatatype, timeDataTypeWithFmt:
		return time.Parse(time.RFC3339, s.String())
	case floatDatatype:
		return strconv.ParseFloat(s.String(), 64)
	case boolDatatype:
		if s == falseString {
			return false, nil
		}
		return true, nil
	case intDatatype:
		return strconv.ParseInt(s.String(), 10, 64)
	case uintDatatype:
		return strconv.ParseUint(s.String(), 10, 64)
	default:
		return nil, fmt.Errorf("%s has unknown data type %s", s, dataType)
	}
}
