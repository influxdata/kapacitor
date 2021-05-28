package coordinator

import (
	"context"
	"errors"
	"time"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/kapacitor/task/backend/executor"
	"github.com/influxdata/kapacitor/task/backend/scheduler"
	"github.com/influxdata/kapacitor/task/taskmodel"
	"go.uber.org/zap"
)

var _ Executor = (*executor.Executor)(nil)

// DefaultLimit is the maximum number of tasks that a given taskd server can own
const DefaultLimit = 1000

// Executor is an abstraction of the task executor with only the functions needed by the coordinator
type Executor interface {
	ManualRun(ctx context.Context, id platform.ID, runID platform.ID) (executor.Promise, error)
	Cancel(ctx context.Context, runID platform.ID) error
}

// Coordinator is the intermediary between the scheduling/executing system and the rest of the task system
type Coordinator struct {
	log *zap.Logger
	sch scheduler.Scheduler
	ex  Executor

	limit int
}

type CoordinatorOption func(*Coordinator)

// SchedulableTask is a wrapper around the Task struct, giving it methods to make it compatible with the scheduler
type SchedulableTask struct {
	*taskmodel.Task
	sch scheduler.Schedule
	lsc time.Time
}

func (t SchedulableTask) ID() scheduler.ID {
	return scheduler.ID(t.Task.ID)
}

// Schedule takes the time a Task is scheduled for and returns a Schedule object
func (t SchedulableTask) Schedule() scheduler.Schedule {
	return t.sch
}

// Offset returns a time.Duration for the Task's offset property
func (t SchedulableTask) Offset() time.Duration {
	return t.Task.Offset
}

// LastScheduled parses the task's LatestCompleted value as a Time object
func (t SchedulableTask) LastScheduled() time.Time {
	return t.lsc
}

func WithLimitOpt(i int) CoordinatorOption {
	return func(c *Coordinator) {
		c.limit = i
	}
}

// NewSchedulableTask transforms an influxdb task to a schedulable task type
func NewSchedulableTask(task *taskmodel.Task) (SchedulableTask, error) {

	if task.Cron == "" && task.Every == "" {
		return SchedulableTask{}, errors.New("invalid cron or every")
	}
	effCron := task.EffectiveCron()
	ts := task.CreatedAt
	if task.LatestScheduled.IsZero() || task.LatestScheduled.Before(task.LatestCompleted) {
		ts = task.LatestCompleted
	} else if !task.LatestScheduled.IsZero() {
		ts = task.LatestScheduled
	}

	var sch scheduler.Schedule
	var err error
	sch, ts, err = scheduler.NewSchedule(effCron, ts)
	if err != nil {
		return SchedulableTask{}, err
	}
	return SchedulableTask{Task: task, sch: sch, lsc: ts}, nil
}

func NewCoordinator(log *zap.Logger, scheduler scheduler.Scheduler, executor Executor, opts ...CoordinatorOption) *Coordinator {
	c := &Coordinator{
		log:   log,
		sch:   scheduler,
		ex:    executor,
		limit: DefaultLimit,
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

// TaskCreated asks the scheduler to schedule the newly created task
func (c *Coordinator) TaskCreated(ctx context.Context, task *taskmodel.Task) error {
	t, err := NewSchedulableTask(task)

	if err != nil {
		return err
	}
	// func new schedulable task
	// catch errors from offset and last scheduled
	if err = c.sch.Schedule(t); err != nil {
		return err
	}

	return nil
}

// TaskUpdated releases the task if it is being disabled, and schedules it otherwise
func (c *Coordinator) TaskUpdated(ctx context.Context, from, to *taskmodel.Task) error {
	sid := scheduler.ID(to.ID)
	t, err := NewSchedulableTask(to)
	if err != nil {
		return err
	}

	// if disabling the task, release it before schedule update
	if to.Status != from.Status && to.Status == string(taskmodel.TaskInactive) {
		if err := c.sch.Release(sid); err != nil && err != taskmodel.ErrTaskNotClaimed {
			return err
		}
	} else {
		if err := c.sch.Schedule(t); err != nil {
			return err
		}
	}

	return nil
}

//TaskDeleted asks the scheduler to release the deleted task
func (c *Coordinator) TaskDeleted(ctx context.Context, id platform.ID) error {
	tid := scheduler.ID(id)
	if err := c.sch.Release(tid); err != nil && err != taskmodel.ErrTaskNotClaimed {
		return err
	}

	return nil
}

// RunCancelled speaks directly to the executor to cancel a task run
func (c *Coordinator) RunCancelled(ctx context.Context, runID platform.ID) error {
	err := c.ex.Cancel(ctx, runID)

	return err
}

// RunRetried speaks directly to the executor to re-try a task run immediately
func (c *Coordinator) RunRetried(ctx context.Context, task *taskmodel.Task, run *taskmodel.Run) error {
	promise, err := c.ex.ManualRun(ctx, task.ID, run.ID)
	if err != nil {
		return taskmodel.ErrRunExecutionError(err)
	}

	<-promise.Done()
	if err = promise.Error(); err != nil {
		return err
	}

	return nil
}

// RunForced speaks directly to the Executor to run a task immediately
func (c *Coordinator) RunForced(ctx context.Context, task *taskmodel.Task, run *taskmodel.Run) error {
	promise, err := c.ex.ManualRun(ctx, task.ID, run.ID)
	if err != nil {
		return taskmodel.ErrRunExecutionError(err)
	}

	<-promise.Done()
	if err = promise.Error(); err != nil {
		return err
	}

	return nil
}
