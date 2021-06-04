package influxdb

import (
	"bytes"
	"io/ioutil"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	imodels "github.com/influxdata/influxdb/models"
)

func mustParseTime(s string) time.Time {
	ts, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return ts
}
func Test_FluxCSV(t *testing.T) {
	x := ioutil.NopCloser(bytes.NewBufferString(`
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string
#group,false,false,true,true,false,false,true,true
,result,table,_start,_stop,_time,_value,_field,_measurement
,_result,0,2021-04-22T16:11:09.536898692Z,2021-04-22T16:13:09.536898692Z,2021-04-22T16:11:14.5346632Z,45,counter,boltdb_reads_total
,_result,0,2021-04-22T16:11:09.536898692Z,2021-04-22T16:13:09.536898692Z,2021-04-22T16:11:24.536408365Z,47,counter,boltdb_reads_total
,_result,0,2021-04-22T16:11:09.536898692Z,2021-04-22T16:13:09.536898692Z,2021-04-22T16:11:34.536392986Z,49,counter,boltdb_reads_total
,_result,1,2021-04-22T16:11:09.536898692Z,2021-04-22T16:13:09.536898692Z,2021-04-22T16:11:14.5346632Z,24,counter,boltdb_writes_total
,_result,1,2021-04-22T16:11:09.536898692Z,2021-04-22T16:13:09.536898692Z,2021-04-22T16:11:24.536408365Z,24,counter,boltdb_writes_total
,_result,1,2021-04-22T16:11:09.536898692Z,2021-04-22T16:13:09.536898692Z,2021-04-22T16:11:34.536392986Z,24,counter,boltdb_writes_total

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string
#group,false,false,true,true,false,false,true,true,true
,result,table,_start,_stop,_time,_value,_field,_measurement,version
,_result,10,2021-04-22T16:11:09.536898692Z,2021-04-22T16:13:09.536898692Z,2021-04-22T16:11:14.5346632Z,1,gauge,go_info,go1.15.2
,_result,10,2021-04-22T16:11:09.536898692Z,2021-04-22T16:13:09.536898692Z,2021-04-22T16:11:24.536408365Z,1,gauge,go_info,go1.15.2
,_result,10,2021-04-22T16:11:09.536898692Z,2021-04-22T16:13:09.536898692Z,2021-04-22T16:11:34.536392986Z,1,gauge,go_info,go1.15.2

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string
#group,false,false,true,true,false,false,true,true
,result,table,_start,_stop,_time,_value,_field,_measurement
,_result,11,2021-04-22T16:11:09.536898692Z,2021-04-22T16:13:09.536898692Z,2021-04-22T16:11:14.5346632Z,26004408,gauge,go_memstats_alloc_bytes
,_result,11,2021-04-22T16:11:09.536898692Z,2021-04-22T16:13:09.536898692Z,2021-04-22T16:11:24.536408365Z,27478576,gauge,go_memstats_alloc_bytes
,_result,11,2021-04-22T16:11:09.536898692Z,2021-04-22T16:13:09.536898692Z,2021-04-22T16:11:34.536392986Z,28957096,gauge,go_memstats_alloc_bytes
,_result,11,2021-04-22T16:11:09.536898692Z,2021-04-22T16:13:09.536898692Z,2021-04-22T16:11:44.534859397Z,30440472,gauge,go_memstats_alloc_bytes
,_result,12,2021-04-22T16:11:09.536898692Z,2021-04-22T16:13:09.536898692Z,2021-04-22T16:11:14.5346632Z,137883344,counter,go_memstats_alloc_bytes_total
,_result,12,2021-04-22T16:11:09.536898692Z,2021-04-22T16:13:09.536898692Z,2021-04-22T16:11:24.536408365Z,139357512,counter,go_memstats_alloc_bytes_total
,_result,12,2021-04-22T16:11:09.536898692Z,2021-04-22T16:13:09.536898692Z,2021-04-22T16:11:34.536392986Z,140836032,counter,go_memstats_alloc_bytes_total
,_result,12,2021-04-22T16:11:09.536898692Z,2021-04-22T16:13:09.536898692Z,2021-04-22T16:11:44.534859397Z,142319408,counter,go_memstats_alloc_bytes_total

`))

	res, err := NewFluxQueryResponse(x)
	if err != nil {
		t.Fatal(err)
	}
	expected := Response{
		Results: []Result{
			{
				Series: []imodels.Row{
					{
						Name:    "boltdb_reads_total",
						Tags:    map[string]string{"_field": "counter"},
						Columns: []string{"time", "_value"},
						Values: [][]interface{}{
							{mustParseTime("2021-04-22T16:11:14.5346632Z"), float64(45)},
							{mustParseTime("2021-04-22T16:11:24.536408365Z"), float64(47)},
							{mustParseTime("2021-04-22T16:11:34.536392986Z"), float64(49)},
						},
					},
					{
						Name:    "boltdb_writes_total",
						Tags:    map[string]string{"_field": "counter"},
						Columns: []string{"time", "_value"},
						Values: [][]interface{}{
							{mustParseTime("2021-04-22T16:11:14.5346632Z"), float64(24)},
							{mustParseTime("2021-04-22T16:11:24.536408365Z"), float64(24)},
							{mustParseTime("2021-04-22T16:11:34.536392986Z"), float64(24)},
						},
					},
					{
						Name:    "go_info",
						Tags:    map[string]string{"_field": "gauge", "version": "go1.15.2"},
						Columns: []string{"time", "_value"},
						Values: [][]interface{}{
							{mustParseTime("2021-04-22T16:11:14.5346632Z"), float64(1)},
							{mustParseTime("2021-04-22T16:11:24.536408365Z"), float64(1)},
							{mustParseTime("2021-04-22T16:11:34.536392986Z"), float64(1)},
						},
					},
					{
						Name:    "go_memstats_alloc_bytes",
						Tags:    map[string]string{"_field": "gauge"},
						Columns: []string{"time", "_value"},
						Values: [][]interface{}{
							{mustParseTime("2021-04-22T16:11:14.5346632Z"), float64(2.6004408e+07)},
							{mustParseTime("2021-04-22T16:11:24.536408365Z"), float64(2.7478576e+07)},
							{mustParseTime("2021-04-22T16:11:34.536392986Z"), float64(2.8957096e+07)},
							{mustParseTime("2021-04-22T16:11:44.534859397Z"), float64(3.0440472e+07)},
						},
					},
					{
						Name:    "go_memstats_alloc_bytes_total",
						Tags:    map[string]string{"_field": "counter"},
						Columns: []string{"time", "_value"},
						Values: [][]interface{}{
							{mustParseTime("2021-04-22T16:11:14.5346632Z"), float64(137883344)},
							{mustParseTime("2021-04-22T16:11:24.536408365Z"), float64(139357512)},
							{mustParseTime("2021-04-22T16:11:34.536392986Z"), float64(140836032)},
							{mustParseTime("2021-04-22T16:11:44.534859397Z"), float64(142319408)},
						},
					},
				},
			},
		},
	}
	if !cmp.Equal(res, &expected) {
		t.Fatal(cmp.Diff(res, &expected))
	}
}
func Test_FluxCSV_Empty(t *testing.T) {
	data := ioutil.NopCloser(bytes.NewBufferString(`#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,string,double
#group,false,false,true,true,false,true,true,false
#default,_result,0,2018-04-17T00:00:00Z,2018-04-17T00:05:00Z,,cpu,A,
,result,table,_start,_stop,_time,_measurement,host,_value`))

	res, err := NewFluxQueryResponse(data)
	if err != nil {
		t.Fatal(err)
	}

	expected := Response{Results: []Result{{}}}
	if !cmp.Equal(res, &expected) {
		t.Fatal(cmp.Diff(res, &expected))
	}
}

func Test_FluxCSV_Error(t *testing.T) {
	data := ioutil.NopCloser(bytes.NewBufferString(`#datatype,string,string
#group,true,true
#default,,
,error,reference
,here is an error,`))

	_, err := NewFluxQueryResponse(data)
	exp := "flux query error: here is an error"
	if err.Error() != exp {
		t.Fatalf("Expected error '%s', but got '%s'", exp, err.Error())
	}

}
