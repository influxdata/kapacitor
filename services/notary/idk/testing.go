package idk

import "github.com/influxdata/kapacitor"

type taskContext struct {
	keys []interface{}
	s    *taskContext
}

type Service struct {
	loggers []interface{}
}

func (s *TaskContext) NewTaskNotary(kv ...interface{}) *taskContext {
	t := &taskContext{
		kv:  keys,
		ctx: s,
	}
	return nil
}

func TaskNotary(n kapacitor.Notary) *context {
	return nil
}
