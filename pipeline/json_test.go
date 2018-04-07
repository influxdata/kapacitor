package pipeline

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/kapacitor/tick/stateful"
	"github.com/influxdata/kapacitor/udf/agent"
)

func TestPipeline_MarshalJSON(t *testing.T) {
	tests := []struct {
		name    string
		script  string
		want    string
		wantErr bool
	}{
		{
			name: "marshal simple pipeline",
			script: `
			var w = stream
				|from()
				|window()
			
			w.period(10s)
			w.every(1s)`,
			want: `{
    "nodes": [
        {
            "typeOf": "stream",
            "id": "0"
        },
        {
            "typeOf": "from",
            "id": "1",
            "where": null,
            "groupBy": null,
            "groupByMeasurement": false,
            "database": "",
            "retentionPolicy": "",
            "measurement": "",
            "round": "0s",
            "truncate": "0s"
        },
        {
            "typeOf": "window",
            "id": "2",
            "align": false,
            "fillPeriod": false,
            "periodCount": 0,
            "everyCount": 0,
            "period": "10s",
            "every": "1s"
        }
    ],
    "edges": [
        {
            "parent": "0",
            "child": "1"
        },
        {
            "parent": "1",
            "child": "2"
        }
    ]
}`,
		},
		{
			name: "chronograf threshold rule",
			script: `var db = 'telegraf'
			
			var rp = 'autogen'
			
			var measurement = 'cpu'
			
			var groupBy = ['host']
			
			var whereFilter = lambda: TRUE
			
			var name = 'Ruley McRuleface'
			
			var idVar = name + ':{{.Group}}'
			
			var message = ' {{.ID}} is  {{.Level}}'
			
			var idTag = 'alertID'
			
			var levelTag = 'level'
			
			var messageField = 'message'
			
			var durationField = 'duration'
			
			var outputDB = 'chronograf'
			
			var outputRP = 'autogen'
			
			var outputMeasurement = 'alerts'
			
			var triggerType = 'threshold'
			
			var crit = 90
			
			var data = stream
				|from()
					.database(db)
					.retentionPolicy(rp)
					.measurement(measurement)
					.groupBy(groupBy)
					.where(whereFilter)
				|eval(lambda: "usage_system")
					.as('value')
			
			var trigger = data
				|alert()
					.crit(lambda: "value" > crit)
					.stateChangesOnly()
					.message(message)
					.id(idVar)
					.idTag(idTag)
					.levelTag(levelTag)
					.messageField(messageField)
					.durationField(durationField)
					.post('http://howdy.local')
			
			trigger
				|influxDBOut()
					.create()
					.database(outputDB)
					.retentionPolicy(outputRP)
					.measurement(outputMeasurement)
					.tag('alertName', name)
					.tag('triggerType', triggerType)
			
			trigger
				|httpOut('output')`,
			want: `{
    "nodes": [
        {
            "typeOf": "stream",
            "id": "0"
        },
        {
            "typeOf": "from",
            "id": "1",
            "where": {
                "expression": {
                    "bool": true,
                    "typeOf": "bool"
                },
                "typeOf": "lambda"
            },
            "groupBy": [
                "host"
            ],
            "groupByMeasurement": false,
            "database": "telegraf",
            "retentionPolicy": "autogen",
            "measurement": "cpu",
            "round": "0s",
            "truncate": "0s"
        },
        {
            "typeOf": "eval",
            "id": "2",
            "as": [
                "value"
            ],
            "tags": null,
            "lambdas": [
                {
                    "expression": {
                        "reference": "usage_system",
                        "typeOf": "reference"
                    },
                    "typeOf": "lambda"
                }
            ],
            "keep": false,
            "keepList": null
        },
        {
            "typeOf": "alert",
            "id": "3",
            "category": "",
            "topic": "",
            "alertId": "Ruley McRuleface:{{.Group}}",
            "message": " {{.ID}} is  {{.Level}}",
            "details": "{{ json . }}",
            "info": null,
            "warn": null,
            "crit": {
                "expression": {
                    "left": {
                        "reference": "value",
                        "typeOf": "reference"
                    },
                    "operator": "\u003e",
                    "right": {
                        "base": 10,
                        "float64": 0,
                        "int64": 90,
                        "isfloat": false,
                        "isint": true,
                        "typeOf": "number"
                    },
                    "typeOf": "binary"
                },
                "typeOf": "lambda"
            },
            "infoReset": null,
            "warnReset": null,
            "critReset": null,
            "useFlapping": false,
            "flapLow": 0,
            "flapHigh": 0,
            "history": 21,
            "levelTag": "level",
            "levelField": "",
            "messageField": "message",
            "durationField": "duration",
            "idTag": "alertID",
            "idField": "",
            "all": false,
            "noRecoveries": false,
            "stateChangesOnly": true,
            "stateChangesOnlyDuration": 0,
            "inhibitors": null,
            "post": [
                {
                    "url": "http://howdy.local",
                    "endpoint": "",
                    "headers": null,
                    "captureResponse": false,
                    "timeout": 0
                }
            ],
            "tcp": null,
            "email": null,
            "exec": null,
            "log": null,
            "victorOps": null,
            "pagerDuty": null,
            "pagerDuty2": null,
            "pushover": null,
            "sensu": null,
            "slack": null,
            "telegram": null,
            "hipChat": null,
            "alerta": null,
            "opsGenie": null,
            "opsGenie2": null,
            "talk": null,
            "mqtt": null,
            "snmpTrap": null,
            "kafka": null
        },
        {
            "typeOf": "httpOut",
            "id": "5",
            "endpoint": "output"
        },
        {
            "typeOf": "influxdbOut",
            "id": "4",
            "cluster": "",
            "database": "chronograf",
            "retentionPolicy": "autogen",
            "measurement": "alerts",
            "writeConsistency": "",
            "precision": "",
            "buffer": 1000,
            "tags": {
                "alertName": "Ruley McRuleface",
                "triggerType": "threshold"
            },
            "create": true,
            "flushInterval": "10s"
        }
    ],
    "edges": [
        {
            "parent": "0",
            "child": "1"
        },
        {
            "parent": "1",
            "child": "2"
        },
        {
            "parent": "2",
            "child": "3"
        },
        {
            "parent": "3",
            "child": "5"
        },
        {
            "parent": "3",
            "child": "4"
        }
    ]
}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := deadman{}

			scope := stateful.NewScope()
			p, err := CreatePipeline(tt.script, StreamEdge, scope, d, nil)
			if err != nil {
				t.Fatal(err)
			}
			got, err := json.MarshalIndent(p, "", "    ")
			if (err != nil) != tt.wantErr {
				t.Errorf("Pipeline.MarshalJSON() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if string(got) != tt.want {
				t.Errorf("Pipeline.MarshalJSON()\ngot:\n%v\nwant:\n%v\n", string(got), tt.want)
			}
		})
	}
}

func TestPipeline_Unmarshal(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		want    []Node
		wantErr bool
	}{
		{
			name: "unmarshal stream|from|window tickscript",
			data: []byte(`{
			"nodes": [
			  {"id": "0", "typeOf": "stream"},
			  {
				"database": "",
				"groupBy": null,
				"groupByMeasurement": false,
				"id": "1",
				"measurement": "",
				"retentionPolicy": "",
				"round": "0s",
				"truncate": "0s",
				"typeOf": "from",
				"where": null
			  },
			  {
				"align": false,
				"every": "1s",
				"everyCount": 0,
				"fillPeriod": false,
				"id": "2",
				"period": "10s",
				"periodCount": 0,
				"typeOf": "window"
			  }
			],
			"edges": [{"parent": "0", "child": "1"}, {"parent": "1", "child": "2"}]
		  }`),
			want: []Node{
				&StreamNode{},
				&FromNode{},
				&WindowNode{
					Period: 10 * time.Second,
					Every:  time.Second,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Pipeline{}
			if err := p.Unmarshal(tt.data); (err != nil) != tt.wantErr {
				t.Errorf("Pipeline.Unmarshal() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			var cmpOptions = cmp.Options{
				cmpopts.IgnoreUnexported(StreamNode{}),
				cmpopts.IgnoreUnexported(WindowNode{}),
				cmpopts.IgnoreUnexported(FromNode{}),
			}
			got := p.sorted
			if !cmp.Equal(got, tt.want, cmpOptions...) {
				t.Errorf("Pipeline.Unmarshal() =-got/+want\n%s", cmp.Diff(got, tt.want, cmpOptions...))
			}
		})
	}
}

func TestPipeline_unmarshalNode(t *testing.T) {
	type args struct {
		data    []byte
		typ     TypeOf
		parents []Node
	}
	tests := []struct {
		name          string
		args          args
		want          Node
		ignoreParents bool // only used for stats node
		wantErr       bool
	}{
		{
			name: "should error when src type has a parent",
			args: args{
				parents: []Node{
					&MockNode{},
				},
				typ: TypeOf{
					Type: "stream",
				},
			},
			wantErr: true,
		},
		{
			name: "add source",
			args: args{
				typ: TypeOf{
					Type: "stream",
				},
			},
			want: &StreamNode{},
		},
		{
			name: "should error when chain function type has more than one parent",
			args: args{
				parents: []Node{
					&MockNode{},
					&MockNode{},
				},
				typ: TypeOf{
					Type: "window",
				},
			},
			wantErr: true,
		},
		{
			name: "unmarshal window",
			args: args{
				parents: []Node{
					&chainnode{
						node: node{
							provides: StreamEdge,
						},
					},
				},
				typ: TypeOf{
					Type: "window",
				},
				data: []byte(`{"id": "1",
					"typeOf": "window",
					"period": "1m",
					"every": "1m",
					"align": true,
					"fillPeriod": true,
					"periodCount": 1,
					"everyCount": 1}`),
			},
			want: &WindowNode{
				Period:         time.Minute,
				Every:          time.Minute,
				AlignFlag:      true,
				FillPeriodFlag: true,
				PeriodCount:    1,
				EveryCount:     1,
			},
		},
		{
			name: "should error when chain function parent isn't a chainnode",
			args: args{
				parents: []Node{
					&MockNode{},
				},
				typ: TypeOf{
					Type: "window",
				},
			},
			wantErr: true,
		},
		{
			name: "should error when source filter type has more than one parent",
			args: args{
				parents: []Node{
					&MockNode{},
					&MockNode{},
				},
				typ: TypeOf{
					Type: "from",
				},
			},
			wantErr: true,
		},
		{
			name: "unmarshal from node",
			args: args{
				parents: []Node{
					newStreamNode(),
				},
				typ: TypeOf{
					Type: "from",
				},
				data: []byte(`{"id": "1",
						"typeOf": "from",
						"where": null,
						"groupBy": null,
						"groupByMeasurement": true,
						"database": "mydb",
						"retentionPolicy": "myrp",
						"measurement": "mymeasurement",
						"truncate": "1m",
						"round": "1m"
						}`),
			},
			want: &FromNode{
				GroupByMeasurementFlag: true,
				Database:               "mydb",
				RetentionPolicy:        "myrp",
				Measurement:            "mymeasurement",
				Truncate:               time.Minute,
				Round:                  time.Minute,
			},
		},
		{
			name: "should error when multi parent type has less than two parents",
			args: args{
				parents: []Node{
					&MockNode{},
				},
				typ: TypeOf{
					Type: "union",
				},
			},
			wantErr: true,
		},
		{
			name: "should error when multi parent type parent isn't a chainnode",
			args: args{
				parents: []Node{
					&MockNode{},
					&MockNode{},
				},
				typ: TypeOf{
					Type: "union",
				},
			},
			wantErr: true,
		},
		{
			name: "should error when union is not able to be unmarshaled",
			args: args{
				parents: []Node{
					&chainnode{},
					&chainnode{},
				},
				typ: TypeOf{
					Type: "union",
				},
			},
			wantErr: true,
		},
		{
			name: "unmarshal union node",
			args: args{
				parents: []Node{
					&chainnode{},
					&chainnode{},
				},
				typ: TypeOf{
					Type: "union",
				},
				data: []byte(`{
						"id": "1",
						"typeOf": "union",
						"rename": "renamed"
					}`),
			},
			want: &UnionNode{
				Rename: "renamed",
			},
		},
		{
			name: "should error when influx function type has more than one parent",
			args: args{
				parents: []Node{
					&MockNode{},
					&MockNode{},
				},
				typ: TypeOf{
					Type: "count",
				},
			},
			wantErr: true,
		},
		{
			name: "should error when influx function parent isn't a chainnode",
			args: args{
				parents: []Node{
					&MockNode{},
				},
				typ: TypeOf{
					Type: "count",
				},
			},
			wantErr: true,
		},
		{
			name: "should error when unable to unmarshal field",
			args: args{
				parents: []Node{
					&chainnode{},
				},
				typ: TypeOf{
					Type: "count",
				},
			},
			wantErr: true,
		},
		{
			name: "unmarshal influxql node",
			args: args{
				parents: []Node{
					&chainnode{},
				},
				typ: TypeOf{
					Type: "count",
				},
				data: []byte(`
				{
					"field": "usage_user",
					"typeOf": "count",
					"id": "1"
				}
				`),
			},
			want: &InfluxQLNode{
				Method: "count",
				As:     "count",
				Field:  "usage_user",
			},
		},
		{
			name: "unmarshal stats",
			args: args{
				parents: []Node{
					&chainnode{},
				},
				data: []byte(`{
                    "typeOf": "stats",
                    "id": "2",
                    "interval": "5s",
                    "align": true 
                }`),
				typ: TypeOf{
					Type: "stats",
				},
			},
			ignoreParents: true,
			want: &StatsNode{
				Interval:   5 * time.Second,
				AlignFlag:  true,
				SourceNode: &node{},
			},
		},
		{
			name: "unmarshal bottom",
			args: args{
				parents: []Node{
					&chainnode{},
				},
				data: []byte(`{
				"typeOf": "bottom",
				"field": "usage_user",
				"tags": null,
				"args": [0]
			}`),
				typ: TypeOf{
					Type: "bottom",
				},
			},
			want: &InfluxQLNode{
				Method:     "bottom",
				Field:      "usage_user",
				As:         "bottom",
				PointTimes: false,
				Args:       []interface{}{int64(0)},
			},
		},
		{
			name: "unmarshal udf",
			args: args{
				parents: []Node{
					&chainnode{},
				},
				typ: TypeOf{
					Type: "udf",
				},
				data: []byte(`{
                        "future": [
                            "15778476m",
                            "years"
                        ],
                        "gigawatts": [
                            1.21
                        ],
                        "id": "0",
                        "martySays": [
                            "Doc!"
                        ],
                        "mph": [
                            88
                        ],
                        "nearClockTower": [
                            true
                        ],
                        "typeOf": "udf",
                        "udfName": "delorean"
                    }`),
			},
			want: &UDFNode{
				Options: []*agent.Option{
					{
						Name: "future",
						Values: []*agent.OptionValue{
							{
								Type: agent.ValueType_DURATION,
								Value: &agent.OptionValue_DurationValue{
									DurationValue: 946708560000000000, // 30 years!
								},
							},
							{
								Type: agent.ValueType_STRING,
								Value: &agent.OptionValue_StringValue{
									StringValue: "years",
								},
							},
						},
					},
					{
						Name: "gigawatts",
						Values: []*agent.OptionValue{
							{
								Type: agent.ValueType_DOUBLE,
								Value: &agent.OptionValue_DoubleValue{
									DoubleValue: 1.21,
								},
							},
						},
					},
					{
						Name: "martySays",
						Values: []*agent.OptionValue{
							{
								Type: agent.ValueType_STRING,
								Value: &agent.OptionValue_StringValue{
									StringValue: "Doc!",
								},
							},
						},
					},
					{
						Name: "mph",
						Values: []*agent.OptionValue{
							{
								Type: agent.ValueType_INT,
								Value: &agent.OptionValue_IntValue{
									IntValue: 88,
								},
							},
						},
					},
					{
						Name: "nearClockTower",
						Values: []*agent.OptionValue{
							{
								Type: agent.ValueType_BOOL,
								Value: &agent.OptionValue_BoolValue{
									BoolValue: true,
								},
							},
						},
					},
				},
				UDFName: "delorean",
			},
		},
		{
			name: "should error when unknown type",
			args: args{
				typ: TypeOf{
					Type: "unknown",
					ID:   0,
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Pipeline{}
			for _, parent := range tt.args.parents {
				p.addSource(parent)
			}
			got, err := p.unmarshalNode(tt.args.data, tt.args.typ, tt.args.parents)
			if (err != nil) != tt.wantErr {
				t.Errorf("Pipeline.unmarshalNode() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			var cmpOptions = cmp.Options{
				cmpopts.IgnoreUnexported(StreamNode{}),
				cmpopts.IgnoreUnexported(WindowNode{}),
				cmpopts.IgnoreUnexported(FromNode{}),
				cmpopts.IgnoreUnexported(UnionNode{}),
				cmpopts.IgnoreUnexported(InfluxQLNode{}),
				cmpopts.IgnoreUnexported(StatsNode{}),
				cmpopts.IgnoreUnexported(UDFNode{}),
				cmpopts.IgnoreUnexported(node{}),
				cmpopts.IgnoreFields(InfluxQLNode{}, "ReduceCreater"),
			}
			if !cmp.Equal(got, tt.want, cmpOptions...) {
				t.Errorf("Pipeline.unmarshalNode() =-got/+want\n%s", cmp.Diff(got, tt.want, cmpOptions...))
			}
			if !tt.ignoreParents && len(got.Parents()) != len(tt.args.parents) {
				t.Errorf("Pipeline.unmarshalNode() num parents = got %d want %d", len(got.Parents()), len(tt.args.parents))
			}
		})
	}
}

func Test_unmarshalFrom(t *testing.T) {
	type args struct {
		data   []byte
		source Node
	}
	tests := []struct {
		name    string
		args    args
		want    Node
		wantErr bool
	}{
		{
			name: "should error when parent isn't a StreamNode",
			args: args{
				source: &MockNode{},
			},
			wantErr: true,
		},
		{
			name: "unmarshal from",
			args: args{
				source: &StreamNode{},
				data: []byte(`{
					"typeOf": "from",
					"id": "2",
					"database": "",
					"groupBy": null,
					"groupByMeasurement": false,
					"measurement": "",
					"retentionPolicy": "",
					"round": "0s",
					"truncate": "0s",
					"typeOf": "from",
					"where": null
					}`),
			},
			want: &FromNode{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !tt.wantErr {
				p := &Pipeline{}
				p1 := tt.args.source
				p.addSource(p1)
			}
			got, err := unmarshalFrom(tt.args.data, tt.args.source)
			if (err != nil) != tt.wantErr {
				t.Errorf("unmarshalFrom() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			var cmpOptions = cmp.Options{
				cmpopts.IgnoreUnexported(FromNode{}),
			}
			if !cmp.Equal(got, tt.want, cmpOptions...) {
				t.Errorf("unmarshalFrom() =-got/+want\n%s", cmp.Diff(got, tt.want, cmpOptions...))
			}
		})
	}
}

func Test_unmarshalQuery(t *testing.T) {
	type args struct {
		data   []byte
		source Node
	}
	tests := []struct {
		name    string
		args    args
		want    Node
		wantErr bool
	}{
		{
			name: "should error when parent isn't a BatchNode",
			args: args{
				source: &MockNode{},
			},
			wantErr: true,
		},
		{
			name: "unmarshal query",
			args: args{
				source: &BatchNode{},
				data: []byte(`{
                    "typeOf": "query",
                    "id": "2",
                    "period": "0s",
                    "every": "0s",
                    "offset": "0s"
                }`),
			},
			want: &QueryNode{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !tt.wantErr {
				p := &Pipeline{}
				p1 := tt.args.source
				p.addSource(p1)
			}
			got, err := unmarshalQuery(tt.args.data, tt.args.source)
			if (err != nil) != tt.wantErr {
				t.Errorf("unmarshalQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			var cmpOptions = cmp.Options{
				cmpopts.IgnoreUnexported(QueryNode{}),
			}
			if !cmp.Equal(got, tt.want, cmpOptions...) {
				t.Errorf("unmarshalQuery() =-got/+want\n%s", cmp.Diff(got, tt.want, cmpOptions...))
			}
		})
	}
}

func Test_unmarshalStats(t *testing.T) {
	type args struct {
		data    []byte
		parents []Node
		typ     TypeOf
	}
	tests := []struct {
		name    string
		args    args
		want    Node
		wantErr bool
	}{
		{
			name: "should error with more than one parent",
			args: args{
				parents: make([]Node, 2),
			},
			wantErr: true,
		},
		{
			name: "should error when parent isn't a node",
			args: args{
				parents: []Node{
					&MockNode{},
				},
			},
			wantErr: true,
		},
		{
			name: "unmarshal stats",
			args: args{
				parents: []Node{
					&node{},
				},
				data: []byte(`{
                    "typeOf": "stats",
                    "id": "2",
                    "interval": "5s",
                    "align": true 
                }`),
				typ: TypeOf{
					Type: "stats",
				},
			},
			want: &StatsNode{
				Interval:  5 * time.Second,
				AlignFlag: true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !tt.wantErr {
				p := &Pipeline{}
				p1 := tt.args.parents[0]
				p.addSource(p1)
			}
			got, err := unmarshalStats(tt.args.data, tt.args.parents, tt.args.typ)
			if (err != nil) != tt.wantErr {
				t.Errorf("unmarshalStats() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			var cmpOptions = cmp.Options{
				cmpopts.IgnoreFields(StatsNode{}, "SourceNode"),
				cmpopts.IgnoreUnexported(StatsNode{}),
			}
			if !cmp.Equal(got, tt.want, cmpOptions...) {
				t.Errorf("unmarshalStats() =-got/+want\n%s", cmp.Diff(got, tt.want, cmpOptions...))
			}
		})
	}
}

func Test_unmarshalTopBottom(t *testing.T) {
	type args struct {
		data    []byte
		parents []Node
		typ     TypeOf
	}
	tests := []struct {
		name    string
		args    args
		want    Node
		wantErr bool
	}{
		{
			name: "should error with more than one parent",
			args: args{
				parents: make([]Node, 2),
			},
			wantErr: true,
		},
		{
			name: "should error when parent isn't a chainnode",
			args: args{
				parents: []Node{
					&MockNode{},
				},
			},
			wantErr: true,
		},
		{
			name: "should error if unable to unmarshal",
			args: args{
				parents: []Node{
					&chainnode{},
				},
			},
			wantErr: true,
		},
		{
			name: "should error if type isn't top or bottom",
			args: args{
				parents: []Node{
					&chainnode{},
				},
				data: []byte(`{
                    "field": "usage_user",
                    "tags": null
                }`),
			},
			wantErr: true,
		},
		{
			name: "unmarshal top",
			args: args{
				parents: []Node{
					&chainnode{},
				},
				data: []byte(`{
                    "typeOf": "top",
                    "field": "usage_user",
                    "tags": null,
                    "args": [0]
                }`),
				typ: TypeOf{
					Type: "top",
				},
			},
			want: &InfluxQLNode{
				Method:     "top",
				Field:      "usage_user",
				As:         "top",
				PointTimes: false,
				Args:       []interface{}{int64(0)},
			},
		},
		{
			name: "unmarshal bottom",
			args: args{
				parents: []Node{
					&chainnode{},
				},
				data: []byte(`{
                    "typeOf": "bottom",
                    "field": "usage_user",
                    "tags": null,
                    "args": [0]
                }`),
				typ: TypeOf{
					Type: "bottom",
				},
			},
			want: &InfluxQLNode{
				Method:     "bottom",
				Field:      "usage_user",
				As:         "bottom",
				PointTimes: false,
				Args:       []interface{}{int64(0)},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !tt.wantErr {
				p := &Pipeline{}
				p1 := tt.args.parents[0]
				p.addSource(p1)
			}
			got, err := unmarshalTopBottom(tt.args.data, tt.args.parents, tt.args.typ)
			if (err != nil) != tt.wantErr {
				t.Errorf("unmarshalTopBottom() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr {
				return
			}
			var cmpOptions = cmp.Options{
				cmpopts.IgnoreFields(InfluxQLNode{}, "ReduceCreater"),
				cmpopts.IgnoreUnexported(InfluxQLNode{}),
			}
			if !cmp.Equal(got, tt.want, cmpOptions...) {
				t.Errorf("unmarshalTopBottom() =-got/+want\n%s", cmp.Diff(got, tt.want, cmpOptions...))
			}
		})
	}
}

func Test_unmarshalUDF(t *testing.T) {
	type args struct {
		data    string
		parents []Node
		typ     TypeOf
	}
	tests := []struct {
		name    string
		args    args
		want    Node
		wantErr bool
	}{
		{
			name: "should error with more than one parent",
			args: args{
				parents: make([]Node, 2),
			},
			wantErr: true,
		},
		{
			name: "unmarshal correctly",
			args: args{
				parents: []Node{
					&MockNode{},
				},
				data: `{
                        "future": [
                            "15778476m",
                            "years"
                        ],
                        "gigawatts": [
                            1.21
                        ],
                        "id": "0",
                        "martySays": [
                            "Doc!"
                        ],
                        "mph": [
                            88
                        ],
                        "nearClockTower": [
                            true
                        ],
                        "typeOf": "udf",
                        "udfName": "delorean"
                    }`,
			},
			want: &UDFNode{
				Options: []*agent.Option{
					{
						Name: "future",
						Values: []*agent.OptionValue{
							{
								Type: agent.ValueType_DURATION,
								Value: &agent.OptionValue_DurationValue{
									DurationValue: 946708560000000000, // 30 years!
								},
							},
							{
								Type: agent.ValueType_STRING,
								Value: &agent.OptionValue_StringValue{
									StringValue: "years",
								},
							},
						},
					},
					{
						Name: "gigawatts",
						Values: []*agent.OptionValue{
							{
								Type: agent.ValueType_DOUBLE,
								Value: &agent.OptionValue_DoubleValue{
									DoubleValue: 1.21,
								},
							},
						},
					},
					{
						Name: "martySays",
						Values: []*agent.OptionValue{
							{
								Type: agent.ValueType_STRING,
								Value: &agent.OptionValue_StringValue{
									StringValue: "Doc!",
								},
							},
						},
					},
					{
						Name: "mph",
						Values: []*agent.OptionValue{
							{
								Type: agent.ValueType_INT,
								Value: &agent.OptionValue_IntValue{
									IntValue: 88,
								},
							},
						},
					},
					{
						Name: "nearClockTower",
						Values: []*agent.OptionValue{
							{
								Type: agent.ValueType_BOOL,
								Value: &agent.OptionValue_BoolValue{
									BoolValue: true,
								},
							},
						},
					},
				},
				UDFName: "delorean",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := unmarshalUDF([]byte(tt.args.data), tt.args.parents, tt.args.typ)
			if (err != nil) != tt.wantErr {
				t.Errorf("unmarshalUDF() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("unmarshalUDF() = %+#v, want %+#v", got, tt.want)
			}
		})
	}
}

var _ Node = &MockNode{}

type MockNode struct {
}

func (m *MockNode) Parents() []Node              { return nil }
func (m *MockNode) Children() []Node             { return nil }
func (m *MockNode) addParent(p Node)             {}
func (m *MockNode) linkChild(c Node)             {}
func (m *MockNode) Desc() string                 { return "" }
func (m *MockNode) Name() string                 { return "" }
func (m *MockNode) SetName(string)               {}
func (m *MockNode) ID() ID                       { return 0 }
func (m *MockNode) setID(ID)                     {}
func (m *MockNode) Wants() EdgeType              { return StreamEdge }
func (m *MockNode) Provides() EdgeType           { return StreamEdge }
func (m *MockNode) validate() error              { return nil }
func (m *MockNode) tMark() bool                  { return true }
func (m *MockNode) setTMark(b bool)              {}
func (m *MockNode) pMark() bool                  { return true }
func (m *MockNode) setPMark(b bool)              {}
func (m *MockNode) setPipeline(*Pipeline)        {}
func (m *MockNode) pipeline() *Pipeline          { return nil }
func (m *MockNode) dot(buf *bytes.Buffer)        {}
func (m *MockNode) MarshalJSON() ([]byte, error) { return nil, nil }
func (m *MockNode) IsQuiet() bool                { return false }
