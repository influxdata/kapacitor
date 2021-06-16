package influxdb

import (
	"encoding/csv"
	"io"

	"github.com/influxdata/kapacitor/istrings"

	"github.com/pkg/errors"
)

type FluxTableMetaData struct {
	TableNumber istrings.IString
	DataTypes   []istrings.IString
	ColumnNames []istrings.IString
	Groups      []bool
}

type FluxCSVEventHandler interface {
	// Error event represents a ',error' table in the flux result, which is special.
	// We use a regular string instead of a istrings.IString because its unlikely
	// that we need  errors to be interned
	Error(err string)

	// GroupStart gives the metadata for a new group of tables
	GroupStart(names []istrings.IString, types []istrings.IString, groups []bool)

	// TableStart marks the start of a table
	TableStart(meta FluxTableMetaData, firstRow []istrings.IString)

	// DataRow is called for each regular data row
	DataRow(meta FluxTableMetaData, row []istrings.IString)

	// TableEnd marks the end of a table
	TableEnd()
}

// FluxCSVEventParser is an event-based parser for flux csv
// it assumes a csv dialect with
// Annotations: []string{"datatype", "group"},
// Delimiter:   ",",
// Header:      true,
type FluxCSVEventParser struct {
	handler       FluxCSVEventHandler
	csvReader     *csv.Reader
	row           []istrings.IString
	colNames      []istrings.IString
	dataTypes     []istrings.IString
	group         []bool
	tableNum      istrings.IString // no need to intern this, as it gets thrown away pretty quickly
	sawTableStart bool
}

var zeroIString = istrings.Get("0")

func NewFluxCSVEventParser(r io.Reader, handler FluxCSVEventHandler) *FluxCSVEventParser {
	q := &FluxCSVEventParser{
		handler:   handler,
		csvReader: csv.NewReader(r),
		tableNum:  zeroIString,
	}
	q.csvReader.FieldsPerRecord = -1
	q.csvReader.ReuseRecord = true
	return q
}

const skipNonDataFluxCols = 3

var (
	errorIString        = istrings.Get("error")
	hashdatatypeIString = istrings.Get("#datatype")
	hashgroupIString    = istrings.Get("#group")
	trueIString         = istrings.Get("true")
)

func (q *FluxCSVEventParser) Parse() error {
	const (
		stateOtherRow = iota
		stateNameRow
		stateFirstDataRow
	)
	state := stateOtherRow
readRow:
	var err error
	q.row, err = istrings.SliceAndErr(q.csvReader.Read())
	if err != nil {
		if err == io.EOF {
			if q.sawTableStart {
				q.handler.TableEnd()
			}
			return nil
		}
		return errors.Wrap(err, "unexpected error while finding next table")
	}

	// check and process error tables
	if len(q.row) > 2 && q.row[1] == errorIString {
		// note the call to Read invalidates the data in q.row but not the size
		row, err := q.csvReader.Read()
		if err != nil || len(row) != len(q.row) {
			if err == io.EOF {
				return errors.Wrap(err, "unexpected EOF in query tables")
			} else if err == nil && len(row) != len(q.row) {
				return errors.Wrap(err, "invalid query data")
			}
			return errors.Wrap(err, "failed to read error value")
		}
		q.handler.Error(row[1])
		return nil
	}

	// skip short rows, this is so we can skip blank lines
	if len(q.row) < skipNonDataFluxCols {
		goto readRow
	}
	// processes based on first column
	switch q.row[0] {
	case istrings.IString{}:
		if len(q.row) <= 5 {
			return errors.New("Unexpectedly few columns")
		}
		if state == stateNameRow {
			newNames := q.row[skipNonDataFluxCols:]
			if cap(q.colNames) < len(newNames) {
				q.colNames = make([]istrings.IString, len(newNames))
			} else {
				q.colNames = q.colNames[:len(newNames)]
			}
			copy(q.colNames, newNames)
			q.handler.GroupStart(q.colNames, q.dataTypes, q.group)
			state = stateFirstDataRow
			goto readRow
		}
		if q.tableNum != q.row[2] { // we have moved on to a new table
			state = stateFirstDataRow
			q.tableNum = q.row[2]
		}
		meta := FluxTableMetaData{
			TableNumber: q.tableNum,
			DataTypes:   q.dataTypes,
			ColumnNames: q.colNames,
			Groups:      q.group,
		}
		if state == stateFirstDataRow {
			if q.sawTableStart {
				q.handler.TableEnd()
			}
			q.sawTableStart = true
			q.handler.TableStart(meta, q.row[skipNonDataFluxCols:])
			q.handler.DataRow(meta, q.row[skipNonDataFluxCols:])
			state = stateOtherRow
			goto readRow
		}
		q.handler.DataRow(meta, q.row[skipNonDataFluxCols:])
		goto readRow
	case hashdatatypeIString:
		newTypes := q.row[skipNonDataFluxCols:]
		if cap(q.dataTypes) < len(newTypes) {
			q.dataTypes = make([]istrings.IString, len(newTypes))
		} else {
			q.dataTypes = q.dataTypes[:len(newTypes)]
		}
		copy(q.dataTypes, newTypes)
		goto readRow
	case hashgroupIString:
		q.group = q.group[:0]
		for _, x := range q.row[skipNonDataFluxCols:] {
			q.group = append(q.group, x == trueIString)
		}
		state = stateNameRow
		goto readRow
	default:
		// if the first column isn't empty, and it isn't #datatype or #group or data row  and we don't need it
		goto readRow
	}
}
