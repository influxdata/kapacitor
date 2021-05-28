package kv

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/kapacitor/task/taskmodel"
)

func Test_newTaskMatchFN(t *testing.T) {
	ct := func(typ string, name string) *taskmodel.Task {
		return &taskmodel.Task{
			Type: typ,
			Name: name,
		}
	}

	const (
		NoTyp = "-"
		NoNam = "-"
	)

	newMatch := func(typ string, name string) taskMatchFn {
		var (
			fil taskmodel.TaskFilter
		)

		if typ != NoTyp {
			fil.Type = &typ
		}

		if name != NoNam {
			fil.Name = &name
		}

		return newTaskMatchFn(fil)
	}

	type test struct {
		name string
		task *taskmodel.Task
		fn   taskMatchFn
		exp  bool
	}

	tests := []struct {
		name  string
		tests []test
	}{
		{
			"match type",
			[]test{
				{
					name: "empty with system type",
					task: ct("", "Foo"),
					fn:   newMatch(taskmodel.TaskSystemType, NoNam),
					exp:  true,
				},
				{
					name: "system with system type",
					task: ct(taskmodel.TaskSystemType, "Foo"),
					fn:   newMatch(taskmodel.TaskSystemType, NoNam),
					exp:  true,
				},
				{
					name: "equal",
					task: ct("other type", "Foo"),
					fn:   newMatch("other type", NoNam),
					exp:  true,
				},
				{
					name: "not type",
					task: ct(taskmodel.TaskSystemType, "Foo"),
					fn:   newMatch("other type", NoNam),
					exp:  false,
				},
			},
		},
		{
			"match name",
			[]test{
				{
					name: "equal",
					task: ct(taskmodel.TaskSystemType, "Foo"),
					fn:   newMatch(NoTyp, "Foo"),
					exp:  true,
				},
				{
					name: "not name",
					task: ct(taskmodel.TaskSystemType, "Foo"),
					fn:   newMatch(NoTyp, "Bar"),
					exp:  false,
				},
			},
		},
		{
			"match name type",
			[]test{
				{
					name: "equal",
					task: ct("check", "Foo"),
					fn:   newMatch("check", "Foo"),
					exp:  true,
				},
				{
					name: "not name",
					task: ct("check", "Foo"),
					fn:   newMatch("check", "Bar"),
					exp:  false,
				},
				{
					name: "not type",
					task: ct("check", "Foo"),
					fn:   newMatch("other", "Foo"),
					exp:  false,
				},
			},
		},
	}
	for _, group := range tests {
		t.Run(group.name, func(t *testing.T) {
			for _, test := range group.tests {
				t.Run(test.name, func(t *testing.T) {
					if got, exp := test.fn(test.task), test.exp; got != exp {
						t.Errorf("unxpected match result: -got/+exp\n%v", cmp.Diff(got, exp))
					}
				})
			}
		})
	}

	t.Run("match returns nil for no filter", func(t *testing.T) {
		fn := newTaskMatchFn(taskmodel.TaskFilter{})
		if fn != nil {
			t.Error("expected nil")
		}
	})
}
