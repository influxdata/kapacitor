package influxdb

import (
	"encoding/csv"
	"io"

	"github.com/pkg/errors"
)

type FluxTableMetaData struct {
	TableNumber string
	DataTypes   []string
	ColumnNames []string
	Groups      []bool
}

type FluxCSVEventHandler interface {
	// Error event represents a ',error' table in the flux result, which is special
	Error(err string)

	// GroupStart gives the metadata for a new group of tables
	GroupStart(names []string, types []string, groups []bool)

	// TableStart marks the start of a table
	TableStart(meta FluxTableMetaData, firstRow []string)

	// DataRow is called for each regular data row
	DataRow(meta FluxTableMetaData, row []string)

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
	row           []string
	colNames      []string
	dataTypes     []string
	group         []bool
	tableNum      string
	sawTableStart bool
}

func NewFluxCSVEventParser(r io.Reader, handler FluxCSVEventHandler) *FluxCSVEventParser {
	q := &FluxCSVEventParser{
		handler:   handler,
		csvReader: csv.NewReader(r),
		tableNum:  "0",
	}
	q.csvReader.FieldsPerRecord = -1
	q.csvReader.ReuseRecord = true
	return q
}

const skipNonDataFluxCols = 3

func (q *FluxCSVEventParser) Parse() error {
	const (
		stateOtherRow = iota
		stateNameRow
		stateFirstDataRow
	)
	state := stateOtherRow
readRow:
	var err error
	q.row, err = q.csvReader.Read()
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
	if len(q.row) > 2 && q.row[1] == "error" {
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
	case "":
		if len(q.row) <= 5 {
			return errors.New("Unexpectedly few columns")
		}
		if state == stateNameRow {
			newNames := q.row[skipNonDataFluxCols:]
			if cap(q.colNames) < len(newNames) {
				q.colNames = make([]string, len(newNames))
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
	case "#datatype":
		newTypes := q.row[skipNonDataFluxCols:]
		if cap(q.dataTypes) < len(newTypes) {
			q.dataTypes = make([]string, len(newTypes))
		} else {
			q.dataTypes = q.dataTypes[:len(newTypes)]
		}
		copy(q.dataTypes, newTypes)
		goto readRow
	case "#group":
		q.group = q.group[:0]
		for _, x := range q.row[skipNonDataFluxCols:] {
			q.group = append(q.group, x == "true")
		}
		state = stateNameRow
		goto readRow
	default:
		// if the first column isn't empty, and it isn't #datatype or #group or data row  and we don't need it
		goto readRow
	}
}
