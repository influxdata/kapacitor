package task_store

import (
	"reflect"
	"testing"

	client "github.com/influxdata/kapacitor/client/v1"
)

func TestDBRPsFromProgram(t *testing.T) {
	type testCase struct {
		name       string
		tickscript string
		dbrps      []client.DBRP
	}

	tt := []testCase{
		{
			name: "one dbrp",
			tickscript: `dbrp "telegraf"."autogen"
			
			stream|from().measurement('m')
			`,
			dbrps: []client.DBRP{
				{
					Database:        "telegraf",
					RetentionPolicy: "auotgen",
				},
			},
		},
		{
			name: "two dbrp",
			tickscript: `dbrp "telegraf"."autogen"
			dbrp "telegraf"."not_autogen"
			
			stream|from().measurement('m')
			`,
			dbrps: []client.DBRP{
				{
					Database:        "telegraf",
					RetentionPolicy: "auotgen",
				},
				{
					Database:        "telegraf",
					RetentionPolicy: "not_autogen",
				},
			},
		},
	}

	for _, tst := range tt {
		t.Run(tst.name, func(t *testing.T) {
			pn, err := newProgramNodeFromTickscript(tst.tickscript)
			if err != nil {
				t.Fatalf("error parsing tickscript: %v", err)
			}
			if exp, got := tst.dbrps, dbrpsFromProgram(pn); reflect.DeepEqual(exp, got) {
				t.Fatalf("DBRPs do not match:\nexp: %v,\ngot %v", exp, got)
			}
		})
	}
}

func TestTaskTypeFromProgram(t *testing.T) {
	type testCase struct {
		name       string
		tickscript string
		taskType   client.TaskType
	}

	tt := []testCase{
		{
			name: "basic stream",
			tickscript: `dbrp "telegraf"."autogen"
			
			stream|from().measurement('m')
			`,
			taskType: client.StreamTask,
		},
		{
			name: "basic batch",
			tickscript: `dbrp "telegraf"."autogen"
			
			batch|query('SELECT * FROM "telegraf"."autogen"."mymeas"')
			`,
			taskType: client.BatchTask,
		},
		{
			name: "var stream",
			tickscript: `dbrp "telegraf"."autogen"
			
			var x = stream|from().measurement('m')
			`,
			taskType: client.StreamTask,
		},
		{
			name: "var batch",
			tickscript: `dbrp "telegraf"."autogen"
			
			var x = batch|query('SELECT * FROM "telegraf"."autogen"."mymeas"')
			`,
			taskType: client.BatchTask,
		},
		{
			name: "mixed type",
			tickscript: `dbrp "telegraf"."autogen"
			
			var x = batch|query('SELECT * FROM "telegraf"."autogen"."mymeas"')
			var y = stream|from().measurement('m')
			`,
			taskType: client.InvalidTask,
		},
		{
			name: "missing batch or stream",
			tickscript: `dbrp "telegraf"."autogen"
			
			var x = testing|query('SELECT * FROM "telegraf"."autogen"."mymeas"')
			`,
			taskType: client.InvalidTask,
		},
	}

	for _, tst := range tt {
		t.Run(tst.name, func(t *testing.T) {
			pn, err := newProgramNodeFromTickscript(tst.tickscript)
			if err != nil {
				t.Fatalf("error parsing tickscript: %v", err)
			}
			if exp, got := tst.taskType, taskTypeFromProgram(pn); exp != got {
				t.Fatalf("TaskTypes do not match:\nexp: %v,\ngot %v", exp, got)
			}
		})
	}
}
