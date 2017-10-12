package pipeline

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/influxdata/kapacitor/tick/stateful"
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
            "id": "0",
            "typeOf": "stream"
        },
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
            "id": "0",
            "typeOf": "stream"
        },
        {
            "database": "telegraf",
            "groupBy": [
                "host"
            ],
            "groupByMeasurement": false,
            "id": "1",
            "measurement": "cpu",
            "retentionPolicy": "autogen",
            "round": "0s",
            "truncate": "0s",
            "typeOf": "from",
            "where": {
                "expression": {
                    "bool": true,
                    "typeOf": "bool"
                },
                "typeOf": "lambda"
            }
        },
        {
            "as": [
                "value"
            ],
            "children": {},
            "keep": false,
            "keepList": null,
            "lambdas": [
                {
                    "expression": {
                        "reference": "usage_system",
                        "typeOf": "reference"
                    },
                    "typeOf": "lambda"
                }
            ],
            "nodeID": "2",
            "quiet": false,
            "tags": null,
            "type": "eval"
        },
        {
            "typeOf": "alert",
            "id": "3",
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
            "post": [
                {
                    "url": "http://howdy.local",
                    "endpoint": "",
                    "headers": null
                }
            ],
            "tcp": null,
            "email": null,
            "exec": null,
            "log": null,
            "victorOps": null,
            "pagerDuty": null,
            "pushover": null,
            "sensu": null,
            "slack": null,
            "telegram": null,
            "hipChat": null,
            "alerta": null,
            "opsGenie": null,
            "talk": null,
            "mqtt": null,
            "snmpTrap": null
        },
        {
            "children": {},
            "endpoint": "output",
            "nodeID": "5",
            "type": "httpOut"
        },
        {
            "buffer": 1000,
            "children": {},
            "cluster": "",
            "create": true,
            "database": "chronograf",
            "flushInterval": 10000000000,
            "measurement": "alerts",
            "nodeID": "4",
            "precision": "",
            "retentionPolicy": "autogen",
            "tag": {
                "alertName": "Ruley McRuleface",
                "triggerType": "threshold"
            },
            "type": "influxdbOut",
            "writeConsistency": ""
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
				fmt.Println(string(got))
				t.Errorf("Pipeline.MarshalJSON() = %v, want %v", string(got), tt.want)
			}
		})
	}
}
