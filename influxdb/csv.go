package influxdb

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/pkg/errors"

	imodels "github.com/influxdata/influxdb/models"
	"github.com/influxdata/kapacitor/models"
)

func NewFluxQueryResponse(r io.Reader) (*Response, error) {
	q := &queryCSVResult{
		csvReader: csv.NewReader(r),
		tableNum:  "0",
		buf:       []imodels.Row{},
		seriesBuf: &imodels.Row{},
	}
	q.csvReader.FieldsPerRecord = -1
	q.csvReader.ReuseRecord = true
	return q.response()
}

// queryCSVResult is the result of a flux query in CSV format
// it assumes a csv dialect with
// Annotations: []string{"datatype", "group"},
// Delimiter:   ",",
// Header:      true,
type queryCSVResult struct {
	csvReader      *csv.Reader
	Row            []string
	ColNames       []string
	colNamesMap    map[string]int
	dataTypes      []string
	group          []bool
	tags           []int
	measurementCol int
	fields         []int
	fieldsLen      int
	defaultVals    []string
	Err            error
	buf            []imodels.Row
	seriesBuf      *imodels.Row
	tableNum       string
}

const skipNonDataFluxCols = 3

func (q *queryCSVResult) response() (*Response, error) {
	const (
		stateOtherRow = iota
		stateNameRow
		stateFirstDataRow
	)
	state := stateOtherRow
readRow:
	q.Row, q.Err = q.csvReader.Read()
	if q.Err != nil {
		if q.Err == io.EOF {
			if len(q.buf) != 0 {
				q.buf = append(q.buf, *q.seriesBuf)
				q.seriesBuf = nil // clear the seriesBuf so the GC can clear it if necessary.
			}
			resp := &Response{Results: []Result{{Series: q.buf}}}
			q.buf = nil // clear the buf so the GC can clear it if necessary.
			return resp, nil
		}
		return nil, q.Err
	}

	// check and process error tables
	if len(q.Row) > 2 && q.Row[1] == "error" {
		row, err := q.csvReader.Read()
		if err != nil || len(row) != len(q.Row) {
			if err == io.EOF {
				return nil, errors.Wrap(q.Err, "unexpected EOF in query tables")
			} else if err == nil && len(row) != len(q.Row) {
				return nil, errors.Wrap(err, "invalid query data")
			}
			return nil, errors.Wrap(err, "failed to read error value")
		}
		return nil, errors.New(q.Row[1])
	}

	// skip short rows, this is so we can skip blank lines
	if len(q.Row) < skipNonDataFluxCols {
		goto readRow
	}
	// processes based on first column
	switch q.Row[0] {
	case "":
		if len(q.Row) <= 5 {
			return nil, errors.New("Unexpectedly few columns")
		}
		if state == stateNameRow {
			q.ColNames = append(q.ColNames[:0], q.Row[skipNonDataFluxCols:]...)

			// replace "_time" that flux uses with "time"
			for i := range q.ColNames {
				if q.ColNames[i] == "_time" {
					q.ColNames[i] = "time"
				}
			}
			q.colNamesMap = make(map[string]int, len(q.Row[skipNonDataFluxCols:]))
			q.tags = q.tags[:0]
			q.fieldsLen = 0
			q.fields = q.fields[:0]
			for i := range q.ColNames {
				cn := q.ColNames[i]
				q.colNamesMap[cn] = i
				switch cn {
				case "_measurement", "_start", "_stop":
				default: // its a tag or a field

					if !q.group[i] { // its a fields
						q.fieldsLen++
						q.fields = append(q.fields, i)
					} else {
						q.tags = append(q.tags, i)
					}
				}
			}
			state = stateFirstDataRow
			goto readRow
		}
		if q.tableNum != q.Row[2] { // we have moved on to a new table
			if q.tableNum != "" {
				q.buf = append(q.buf, *q.seriesBuf)
				q.seriesBuf = &imodels.Row{}
			}
			if i, ok := q.colNamesMap["_measurement"]; ok {
				q.seriesBuf.Name = q.Row[i+skipNonDataFluxCols]
			}
			state = stateFirstDataRow
			q.tableNum = q.Row[2]
		}
		values := make([]interface{}, 0, q.fieldsLen)
		if state == stateFirstDataRow {
			tags := make(models.Tags, len(q.Row)-skipNonDataFluxCols)
			// add the tags from the row
			for _, i := range q.tags {
				tags[q.ColNames[i]] = q.Row[i+skipNonDataFluxCols]
			}
			// add the fields and Column names from the row
			for _, i := range q.fields {
				q.seriesBuf.Columns = append(q.seriesBuf.Columns, q.ColNames[i])
				var err error
				val, err := q.convert(i)
				if err != nil {
					return nil, err
				}
				values = append(values, val)
			}
			q.seriesBuf.Tags = tags
			q.seriesBuf.Values = append(q.seriesBuf.Values, values)
			if i, ok := q.colNamesMap["_measurement"]; ok {
				q.seriesBuf.Name = q.Row[i+skipNonDataFluxCols]
			}
			state = stateOtherRow
			goto readRow
		}
		// convert and append the fields
		for _, i := range q.fields {
			var err error
			val, err := q.convert(i)
			if err != nil {
				return nil, err
			}
			values = append(values, val)
		}
		q.seriesBuf.Values = append(q.seriesBuf.Values, values)
		goto readRow
	case "#datatype":
		// parse datatypes here
		q.dataTypes = append(q.dataTypes[:0], q.Row[skipNonDataFluxCols:]...)
		goto readRow
	case "#group":
		q.group = q.group[:0]
		for _, x := range q.Row[skipNonDataFluxCols:] {
			q.group = append(q.group, x == "true")
		}
		state = stateNameRow
		goto readRow
	default:
		// if the first column isn't empty, and it isn't #datatype or #group or data row  and we don't need it
		goto readRow
	}
}

// if it is part of the group key and not a time value named either _time, _start, or _stop

func (q *queryCSVResult) convert(column int) (interface{}, error) {
	const (
		stringDatatype      = "string"
		timeDatatype        = "dateTime"
		floatDatatype       = "double"
		boolDatatype        = "boolean"
		intDatatype         = "long"
		uintDatatype        = "unsignedLong"
		timeDataTypeWithFmt = "dateTime:RFC3339"
	)
	s := q.Row[column+skipNonDataFluxCols]
	switch q.dataTypes[column] {
	case stringDatatype:
		return s, nil
	case timeDatatype, timeDataTypeWithFmt:
		return time.Parse(time.RFC3339, s)
	case floatDatatype:
		return strconv.ParseFloat(s, 64)
	case boolDatatype:
		if s == "false" {
			return false, nil
		}
		return true, nil
	case intDatatype:
		return strconv.ParseInt(s, 10, 64)
	case uintDatatype:
		return strconv.ParseUint(s, 10, 64)
	default:
		return nil, fmt.Errorf("%s has unknown data type %s", s, q.dataTypes[column])
	}
}
