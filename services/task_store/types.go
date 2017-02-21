package task_store

import "github.com/influxdata/kapacitor/services/events"

type Event int

const (
	TaskCreated Event = iota
	TaskDeleted
	TaskEnabled
	TaskDisabled
)

const TaskEventType events.Type = "task"

type TaskEvent struct {
	Task  string
	Event Event
}

func (TaskEvent) Type() events.Type {
	return TaskEventType
}
