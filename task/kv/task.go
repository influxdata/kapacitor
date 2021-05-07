package kv

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/snowflake"
	"github.com/influxdata/kapacitor/services/storage"
	"github.com/influxdata/kapacitor/task/options"
	"github.com/influxdata/kapacitor/task/taskmodel"
)

// Task Storage Schema
// taskBucket:
//   <taskID>: task data storage
// taskRunBucket:
//   <taskID>/<runID>: run data storage
//   <taskID>/manualRuns: list of runs to run manually
//   <taskID>/latestCompleted: run data for the latest completed run of a task

// We may want to add a <taskName>/<taskID> index to allow us to look up tasks by task name.

var (
	taskPrefix    = "tasksv1"
	taskRunPrefix = "taskRunsv1"
)

type Service struct {
	store       StorageService
	kv          storage.Interface
	clock       clock.Clock
	IDGenerator platform.IDGenerator
}

type StorageService interface {
	Store(string) storage.Interface
}

type Option func(s *Service)

func WithClock(c clock.Clock) Option {
	return func(s *Service) {
		s.clock = c
	}
}

func New(kv StorageService, opts ...Option) *Service {
	s := &Service{
		store:       kv,
		clock:       clock.New(),
		IDGenerator: snowflake.NewIDGenerator(),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *Service) Open() error {
	s.kv = s.store.Store("fluxtasks")
	return nil
}

func (s *Service) Close() error {
	return nil
}

var _ taskmodel.TaskService = (*Service)(nil)

type kvTask struct {
	ID              platform.ID            `json:"id"`
	Type            string                 `json:"type,omitempty"`
	OwnerUsername   string                 `json:"ownerID"`
	Name            string                 `json:"name"`
	Description     string                 `json:"description,omitempty"`
	Status          string                 `json:"status"`
	Flux            string                 `json:"flux"`
	Every           string                 `json:"every,omitempty"`
	Cron            string                 `json:"cron,omitempty"`
	LastRunStatus   string                 `json:"lastRunStatus,omitempty"`
	LastRunError    string                 `json:"lastRunError,omitempty"`
	Offset          influxdb.Duration      `json:"offset,omitempty"`
	LatestCompleted time.Time              `json:"latestCompleted,omitempty"`
	LatestScheduled time.Time              `json:"latestScheduled,omitempty"`
	LatestSuccess   time.Time              `json:"latestSuccess,omitempty"`
	LatestFailure   time.Time              `json:"latestFailure,omitempty"`
	CreatedAt       time.Time              `json:"createdAt,omitempty"`
	UpdatedAt       time.Time              `json:"updatedAt,omitempty"`
	Metadata        map[string]interface{} `json:"metadata,omitempty"`
}

func kvToInfluxTask(k *kvTask) *taskmodel.Task {
	return &taskmodel.Task{
		ID:              k.ID,
		Type:            k.Type,
		OwnerUsername:   k.OwnerUsername,
		Name:            k.Name,
		Description:     k.Description,
		Status:          k.Status,
		Flux:            k.Flux,
		Every:           k.Every,
		Cron:            k.Cron,
		LastRunStatus:   k.LastRunStatus,
		LastRunError:    k.LastRunError,
		Offset:          k.Offset.Duration,
		LatestCompleted: k.LatestCompleted,
		LatestScheduled: k.LatestScheduled,
		LatestSuccess:   k.LatestSuccess,
		LatestFailure:   k.LatestFailure,
		CreatedAt:       k.CreatedAt,
		UpdatedAt:       k.UpdatedAt,
		Metadata:        k.Metadata,
	}
}

// FindTaskByID returns a single task
func (s *Service) FindTaskByID(ctx context.Context, id platform.ID) (*taskmodel.Task, error) {
	var t *taskmodel.Task
	err := s.kv.View(func(tx storage.ReadOnlyTx) error {
		task, err := s.findTaskByID(ctx, tx, id)
		if err != nil {
			return err
		}
		t = task
		return nil
	})
	if err != nil {
		return nil, err
	}

	return t, nil
}

func IsNotFound(err error) bool {
	return err == storage.ErrNoKeyExists
}

// findTaskByID is an internal method used to do any action with tasks internally
// that do not require authorization.
func (s *Service) findTaskByID(ctx context.Context, tx storage.ReadOnlyTx, id platform.ID) (*taskmodel.Task, error) {
	b := &wrappedReadTx{
		tx:     tx,
		prefix: taskPrefix,
	}

	v, err := b.Get(id.String())
	if IsNotFound(err) {
		return nil, taskmodel.ErrTaskNotFound
	}
	if err != nil {
		return nil, err
	}
	kvTask := &kvTask{}
	if err := json.Unmarshal(v.Value, kvTask); err != nil {
		return nil, taskmodel.ErrInternalTaskServiceError(err)
	}

	t := kvToInfluxTask(kvTask)

	if t.LatestCompleted.IsZero() {
		t.LatestCompleted = t.CreatedAt
	}

	return t, nil
}

// FindTasks returns a list of tasks that match a filter (limit 100) and the total count
// of matching tasks.
func (s *Service) FindTasks(ctx context.Context, filter taskmodel.TaskFilter) ([]*taskmodel.Task, int, error) {
	var ts []*taskmodel.Task
	err := s.kv.View(func(tx storage.ReadOnlyTx) error {
		tasks, _, err := s.findTasks(ctx, tx, filter)
		if err != nil {
			return err
		}
		ts = tasks
		return nil
	})
	if err != nil {
		return nil, 0, err
	}

	return ts, len(ts), nil
}

func (s *Service) findTasks(ctx context.Context, tx storage.ReadOnlyTx, filter taskmodel.TaskFilter) ([]*taskmodel.Task, int, error) {
	// complain about limits
	if filter.Limit < 0 {
		return nil, 0, taskmodel.ErrPageSizeTooSmall
	}
	if filter.Limit > taskmodel.TaskMaxPageSize {
		return nil, 0, taskmodel.ErrPageSizeTooLarge
	}
	if filter.Limit == 0 {
		filter.Limit = taskmodel.TaskDefaultPageSize
	}

	return s.findAllTasks(ctx, tx, filter)
}

type taskMatchFn func(*taskmodel.Task) bool

// newTaskMatchFn returns a function for validating
// a task matches the filter. Will return nil if
// the filter should match all tasks.
func newTaskMatchFn(f taskmodel.TaskFilter) func(t *taskmodel.Task) bool {
	var fn taskMatchFn

	if f.Type != nil {
		expected := *f.Type
		prevFn := fn
		fn = func(t *taskmodel.Task) bool {
			res := prevFn == nil || prevFn(t)
			return res &&
				((expected == taskmodel.TaskSystemType && (t.Type == taskmodel.TaskSystemType || t.Type == "")) || expected == t.Type)
		}
	}

	if f.Name != nil {
		expected := *f.Name
		prevFn := fn
		fn = func(t *taskmodel.Task) bool {
			res := prevFn == nil || prevFn(t)
			return res && (expected == t.Name)
		}
	}

	if f.Status != nil {
		prevFn := fn
		fn = func(t *taskmodel.Task) bool {
			res := prevFn == nil || prevFn(t)
			return res && (t.Status == *f.Status)
		}
	}

	if f.Username != nil {
		prevFn := fn
		fn = func(t *taskmodel.Task) bool {
			res := prevFn == nil || prevFn(t)
			return res && t.OwnerUsername == *f.Username
		}
	}

	return fn
}

// findAllTasks is a subset of the find tasks function. Used for cleanliness.
// This function should only be executed internally because it doesn't force organization or user filtering.
// Enforcing filters should be done in a validation layer.
func (s *Service) findAllTasks(ctx context.Context, tx storage.ReadOnlyTx, filter taskmodel.TaskFilter) ([]*taskmodel.Task, int, error) {
	var ts []*taskmodel.Task

	taskBucket := &wrappedReadTx{
		tx:     tx,
		prefix: taskPrefix,
	}

	kvList, err := taskBucket.List("")
	if err != nil {
		return nil, 0, err
	}

	if filter.After != nil {
		n := sort.Search(len(kvList), func(i int) bool {
			return kvList[i].Key > taskPrefix+"/"+filter.After.String()
		})
		kvList = kvList[n:]
	}

	matchFn := newTaskMatchFn(filter)

	for _, v := range kvList {
		kvTask := &kvTask{}
		err = json.Unmarshal(v.Value, &kvTask)
		if err != nil {
			return nil, 0, taskmodel.ErrInternalTaskServiceError(err)
		}
		t := kvToInfluxTask(kvTask)
		if matchFn == nil || matchFn(t) {
			ts = append(ts, t)

			if len(ts) >= filter.Limit {
				break
			}
		}
	}
	return ts, len(ts), err
}

// CreateTask creates a new task.
// The owner of the task is inferred from the authorizer associated with ctx.
func (s *Service) CreateTask(ctx context.Context, tc taskmodel.TaskCreate) (*taskmodel.Task, error) {
	var t *taskmodel.Task
	err := s.kv.Update(func(tx storage.Tx) error {
		task, err := s.createTask(ctx, tx, tc)
		if err != nil {
			return err
		}
		t = task
		return nil
	})

	return t, err
}

func (s *Service) createTask(ctx context.Context, tx storage.Tx, tc taskmodel.TaskCreate) (*taskmodel.Task, error) {

	opts, err := options.FromScriptAST(tc.Flux)
	if err != nil {
		return nil, taskmodel.ErrTaskOptionParse(err)
	}

	if tc.Status == "" {
		tc.Status = string(taskmodel.TaskActive)
	}

	createdAt := s.clock.Now().Truncate(time.Second).UTC()
	task := &taskmodel.Task{
		ID:              s.IDGenerator.ID(),
		Type:            tc.Type,
		OwnerUsername:   tc.OwnerUsername,
		Metadata:        tc.Metadata,
		Name:            opts.Name,
		Description:     tc.Description,
		Status:          tc.Status,
		Flux:            tc.Flux,
		Every:           opts.Every.String(),
		Cron:            opts.Cron,
		CreatedAt:       createdAt,
		LatestCompleted: createdAt,
		LatestScheduled: createdAt,
	}

	if opts.Offset != nil {
		off, err := time.ParseDuration(opts.Offset.String())
		if err != nil {
			return nil, taskmodel.ErrTaskTimeParse(err)
		}
		task.Offset = off

	}

	taskBucket := wrappedTx{
		tx:     tx,
		prefix: taskPrefix,
	}

	taskBytes, err := json.Marshal(task)
	if err != nil {
		return nil, taskmodel.ErrInternalTaskServiceError(err)
	}

	// write the task
	err = taskBucket.Put(task.ID.String(), taskBytes)
	if err != nil {
		return nil, taskmodel.ErrUnexpectedTaskBucketErr(err)
	}

	// Note InfluxDB 2.x has a no-op audit logger here - we ignore it

	return task, nil
}

// UpdateTask updates a single task with changeset.
func (s *Service) UpdateTask(ctx context.Context, id platform.ID, upd taskmodel.TaskUpdate) (*taskmodel.Task, error) {
	var t *taskmodel.Task
	err := s.kv.Update(func(tx storage.Tx) error {
		task, err := s.updateTask(ctx, tx, id, upd)
		if err != nil {
			return err
		}
		t = task
		return nil
	})
	if err != nil {
		return nil, err
	}

	return t, nil
}

func (s *Service) updateTask(ctx context.Context, tx storage.Tx, id platform.ID, upd taskmodel.TaskUpdate) (*taskmodel.Task, error) {
	// retrieve the task
	task, err := s.findTaskByID(ctx, tx, id)
	if err != nil {
		return nil, err
	}

	updatedAt := s.clock.Now().UTC()

	// update the flux script
	if !upd.Options.IsZero() || upd.Flux != nil {
		if err = upd.UpdateFlux(task.Flux); err != nil {
			return nil, err
		}
		task.Flux = *upd.Flux

		opts, err := options.FromScriptAST(*upd.Flux)
		if err != nil {
			return nil, taskmodel.ErrTaskOptionParse(err)
		}
		task.Name = opts.Name
		task.Every = opts.Every.String()
		task.Cron = opts.Cron

		var off time.Duration
		if opts.Offset != nil {
			off, err = time.ParseDuration(opts.Offset.String())
			if err != nil {
				return nil, taskmodel.ErrTaskTimeParse(err)
			}
		}
		task.Offset = off
		task.UpdatedAt = updatedAt
	}

	if upd.Description != nil {
		task.Description = *upd.Description
		task.UpdatedAt = updatedAt
	}

	if upd.Status != nil && task.Status != *upd.Status {
		task.Status = *upd.Status
		task.UpdatedAt = updatedAt

		// task is transitioning from inactive to active, ensure scheduled and completed are updated
		if task.Status == taskmodel.TaskStatusActive {
			updatedAtTrunc := updatedAt.Truncate(time.Second).UTC()
			task.LatestCompleted = updatedAtTrunc
			task.LatestScheduled = updatedAtTrunc
		}
	}

	if upd.Metadata != nil {
		task.Metadata = upd.Metadata
		task.UpdatedAt = updatedAt
	}

	if upd.LatestCompleted != nil {
		// make sure we only update latest completed one way
		tlc := task.LatestCompleted
		ulc := *upd.LatestCompleted

		if !ulc.IsZero() && ulc.After(tlc) {
			task.LatestCompleted = *upd.LatestCompleted
		}
	}

	if upd.LatestScheduled != nil {
		// make sure we only update latest scheduled one way
		if upd.LatestScheduled.After(task.LatestScheduled) {
			task.LatestScheduled = *upd.LatestScheduled
		}
	}

	if upd.LatestSuccess != nil {
		// make sure we only update latest success one way
		tlc := task.LatestSuccess
		ulc := *upd.LatestSuccess

		if !ulc.IsZero() && ulc.After(tlc) {
			task.LatestSuccess = *upd.LatestSuccess
		}
	}

	if upd.LatestFailure != nil {
		// make sure we only update latest failure one way
		tlc := task.LatestFailure
		ulc := *upd.LatestFailure

		if !ulc.IsZero() && ulc.After(tlc) {
			task.LatestFailure = *upd.LatestFailure
		}
	}

	if upd.LastRunStatus != nil {
		task.LastRunStatus = *upd.LastRunStatus
		if *upd.LastRunStatus == "failed" && upd.LastRunError != nil {
			task.LastRunError = *upd.LastRunError
		} else {
			task.LastRunError = ""
		}
	}

	// save the updated task
	bucket := wrappedTx{
		tx:     tx,
		prefix: taskPrefix,
	}
	if err != nil {
		return nil, taskmodel.ErrUnexpectedTaskBucketErr(err)
	}

	taskBytes, err := json.Marshal(task)
	if err != nil {
		return nil, taskmodel.ErrInternalTaskServiceError(err)
	}

	err = bucket.Put(id.String(), taskBytes)
	if err != nil {
		return nil, taskmodel.ErrUnexpectedTaskBucketErr(err)
	}

	// Note InfluxDB 2.x has a no-op audit logger here - we ignore it

	return task, nil
}

// DeleteTask removes a task by ID and purges all associated data and scheduled runs.
func (s *Service) DeleteTask(ctx context.Context, id platform.ID) error {
	err := s.kv.Update(func(tx storage.Tx) error {
		err := s.deleteTask(ctx, tx, id)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *Service) deleteTask(ctx context.Context, tx storage.Tx, id platform.ID) error {
	taskBucket := wrappedTx{
		tx:     tx,
		prefix: taskPrefix,
	}
	runBucket := wrappedTx{
		tx:     tx,
		prefix: taskRunPrefix,
	}

	// retrieve the task
	task, err := s.findTaskByID(ctx, tx, id)
	if err != nil {
		return err
	}

	// remove latest completed
	lastCompletedKey := taskLatestCompletedKey(task.ID)

	if err := runBucket.Delete(lastCompletedKey); err != nil {
		return taskmodel.ErrUnexpectedTaskBucketErr(err)
	}

	// remove the runs
	runs, _, err := s.findRuns(ctx, tx, taskmodel.RunFilter{Task: task.ID})
	if err != nil {
		return err
	}

	for _, run := range runs {
		key := taskRunKey(task.ID, run.ID)

		if err := runBucket.Delete(key); err != nil {
			return taskmodel.ErrUnexpectedTaskBucketErr(err)
		}
	}

	if err := taskBucket.Delete(task.ID.String()); err != nil {
		return taskmodel.ErrUnexpectedTaskBucketErr(err)
	}

	// Note InfluxDB 2.x has a no-op audit logger here - we ignore it
	return nil
}

// FindLogs returns logs for a run.
func (s *Service) FindLogs(ctx context.Context, filter taskmodel.LogFilter) ([]*taskmodel.Log, int, error) {
	var logs []*taskmodel.Log
	err := s.kv.View(func(tx storage.ReadOnlyTx) error {
		ls, _, err := s.findLogs(ctx, tx, filter)
		if err != nil {
			return err
		}
		logs = ls
		return nil
	})
	if err != nil {
		return nil, 0, err
	}

	return logs, len(logs), nil
}

func (s *Service) findLogs(ctx context.Context, tx storage.ReadOnlyTx, filter taskmodel.LogFilter) ([]*taskmodel.Log, int, error) {
	if filter.Run != nil {
		r, err := s.findRunByID(ctx, tx, filter.Task, *filter.Run)
		if err != nil {
			return nil, 0, err
		}
		rtn := make([]*taskmodel.Log, len(r.Log))
		for i := 0; i < len(r.Log); i++ {
			rtn[i] = &r.Log[i]
		}
		return rtn, len(rtn), nil
	}

	runs, _, err := s.findRuns(ctx, tx, taskmodel.RunFilter{Task: filter.Task})
	if err != nil {
		return nil, 0, err
	}
	var logs []*taskmodel.Log
	for _, run := range runs {
		for i := 0; i < len(run.Log); i++ {
			logs = append(logs, &run.Log[i])

		}
	}
	return logs, len(logs), nil
}

// FindRuns returns a list of runs that match a filter and the total count of returned runs.
func (s *Service) FindRuns(ctx context.Context, filter taskmodel.RunFilter) ([]*taskmodel.Run, int, error) {
	var runs []*taskmodel.Run
	err := s.kv.View(func(tx storage.ReadOnlyTx) error {
		rs, _, err := s.findRuns(ctx, tx, filter)
		if err != nil {
			return err
		}
		runs = rs
		return nil
	})
	if err != nil {
		return nil, 0, err
	}

	return runs, len(runs), nil
}

func (s *Service) findRuns(ctx context.Context, tx storage.ReadOnlyTx, filter taskmodel.RunFilter) ([]*taskmodel.Run, int, error) {
	if filter.Limit == 0 {
		filter.Limit = taskmodel.TaskDefaultPageSize
	}

	if filter.Limit < 0 || filter.Limit > taskmodel.TaskMaxPageSize {
		return nil, 0, taskmodel.ErrOutOfBoundsLimit
	}
	parsedFilterAfterTime := time.Time{}
	parsedFilterBeforeTime := time.Now().UTC()
	var err error
	if len(filter.AfterTime) > 0 {
		parsedFilterAfterTime, err = time.Parse(time.RFC3339, filter.AfterTime)
		if err != nil {
			return nil, 0, err
		}
	}
	if filter.BeforeTime != "" {
		parsedFilterBeforeTime, err = time.Parse(time.RFC3339, filter.BeforeTime)
		if err != nil {
			return nil, 0, err
		}
	}

	var runs []*taskmodel.Run
	// manual runs
	manualRuns, err := s.manualRuns(ctx, tx, filter.Task)
	if err != nil {
		return nil, 0, err
	}
	for _, run := range manualRuns {
		if run.ScheduledFor.After(parsedFilterAfterTime) && run.ScheduledFor.Before(parsedFilterBeforeTime) {
			runs = append(runs, run)
		}
		if len(runs) >= filter.Limit {
			return runs, len(runs), nil
		}
	}

	// append currently running
	currentlyRunning, err := s.currentlyRunning(ctx, tx, filter.Task)
	if err != nil {
		return nil, 0, err
	}
	for _, run := range currentlyRunning {
		if run.ScheduledFor.After(parsedFilterAfterTime) && run.ScheduledFor.Before(parsedFilterBeforeTime) {
			runs = append(runs, run)
		}
		if len(runs) >= filter.Limit {
			return runs, len(runs), nil
		}
	}

	return runs, len(runs), nil
}

// FindRunByID returns a single run.
func (s *Service) FindRunByID(ctx context.Context, taskID, runID platform.ID) (*taskmodel.Run, error) {
	var run *taskmodel.Run
	err := s.kv.View(func(tx storage.ReadOnlyTx) error {
		r, err := s.findRunByID(ctx, tx, taskID, runID)
		if err != nil {
			return err
		}
		run = r
		return nil
	})
	if err != nil {
		return nil, err
	}

	return run, nil
}

func (s *Service) findRunByID(ctx context.Context, tx storage.ReadOnlyTx, taskID, runID platform.ID) (*taskmodel.Run, error) {
	bucket := wrappedReadTx{
		tx:     tx,
		prefix: taskRunPrefix,
	}

	key := taskRunKey(taskID, runID)
	runBytes, err := bucket.Get(key)
	if err != nil {
		if IsNotFound(err) {
			return nil, taskmodel.ErrRunNotFound
		}
		return nil, taskmodel.ErrUnexpectedTaskBucketErr(err)
	}
	run := &taskmodel.Run{}
	err = json.Unmarshal(runBytes.Value, run)
	if err != nil {
		return nil, taskmodel.ErrInternalTaskServiceError(err)
	}

	return run, nil
}

// CancelRun cancels a currently running run.
func (s *Service) CancelRun(ctx context.Context, taskID, runID platform.ID) error {
	err := s.kv.Update(func(tx storage.Tx) error {
		err := s.cancelRun(ctx, tx, taskID, runID)
		if err != nil {
			return err
		}
		return nil
	})
	return err
}

func (s *Service) cancelRun(ctx context.Context, tx storage.Tx, taskID, runID platform.ID) error {
	// get the run
	run, err := s.findRunByID(ctx, tx, taskID, runID)
	if err != nil {
		return err
	}

	// set status to canceled
	run.Status = "canceled"

	// save
	bucket := wrappedTx{
		tx:     tx,
		prefix: taskRunPrefix,
	}

	runBytes, err := json.Marshal(run)
	if err != nil {
		return taskmodel.ErrInternalTaskServiceError(err)
	}

	runKey := taskRunKey(taskID, runID)

	if err := bucket.Put(runKey, runBytes); err != nil {
		return taskmodel.ErrUnexpectedTaskBucketErr(err)
	}

	return nil
}

// RetryRun creates and returns a new run (which is a retry of another run).
func (s *Service) RetryRun(ctx context.Context, taskID, runID platform.ID) (*taskmodel.Run, error) {
	var r *taskmodel.Run
	err := s.kv.Update(func(tx storage.Tx) error {
		run, err := s.retryRun(ctx, tx, taskID, runID)
		if err != nil {
			return err
		}
		r = run
		return nil
	})
	return r, err
}

func (s *Service) retryRun(ctx context.Context, tx storage.Tx, taskID, runID platform.ID) (*taskmodel.Run, error) {
	// find the run
	r, err := s.findRunByID(ctx, tx, taskID, runID)
	if err != nil {
		return nil, err
	}

	r.ID = s.IDGenerator.ID()
	r.Status = taskmodel.RunScheduled.String()
	r.StartedAt = time.Time{}
	r.FinishedAt = time.Time{}
	r.RequestedAt = time.Time{}

	// add a clean copy of the run to the manual runs
	bucket := wrappedTx{
		tx:     tx,
		prefix: taskRunPrefix,
	}

	key := taskManualRunKey(taskID)

	runs := make([]*taskmodel.Run, 0)
	runsBytes, err := bucket.Get(key)
	if err != nil {
		if IsNotFound(err) {
			return nil, taskmodel.ErrRunNotFound
		}
		return nil, taskmodel.ErrUnexpectedTaskBucketErr(err)

	}

	if runsBytes != nil {
		if err := json.Unmarshal(runsBytes.Value, &runs); err != nil {
			return nil, taskmodel.ErrInternalTaskServiceError(err)
		}
	}

	runs = append(runs, r)

	// save manual runs
	newRunBytes, err := json.Marshal(runs)
	if err != nil {
		return nil, taskmodel.ErrInternalTaskServiceError(err)
	}

	if err := bucket.Put(key, newRunBytes); err != nil {
		return nil, taskmodel.ErrUnexpectedTaskBucketErr(err)
	}

	return r, nil
}

// ForceRun forces a run to occur with unix timestamp scheduledFor, to be executed as soon as possible.
// The value of scheduledFor may or may not align with the task's schedule.
func (s *Service) ForceRun(ctx context.Context, taskID platform.ID, scheduledFor int64) (*taskmodel.Run, error) {
	var r *taskmodel.Run
	err := s.kv.Update(func(tx storage.Tx) error {
		run, err := s.forceRun(ctx, tx, taskID, scheduledFor)
		if err != nil {
			return err
		}
		r = run
		return nil
	})
	return r, err
}

func (s *Service) forceRun(ctx context.Context, tx storage.Tx, taskID platform.ID, scheduledFor int64) (*taskmodel.Run, error) {
	// create a run
	t := time.Unix(scheduledFor, 0).UTC()
	r := &taskmodel.Run{
		ID:           s.IDGenerator.ID(),
		TaskID:       taskID,
		Status:       taskmodel.RunScheduled.String(),
		RequestedAt:  time.Now().UTC(),
		ScheduledFor: t,
		Log:          []taskmodel.Log{},
	}

	// add a clean copy of the run to the manual runs
	bucket := wrappedTx{
		tx:     tx,
		prefix: taskRunPrefix,
	}

	runs, err := s.manualRuns(ctx, tx, taskID)
	if err != nil {
		return nil, err
	}

	// check to see if this run is already queued
	for _, run := range runs {
		if run.ScheduledFor == r.ScheduledFor {
			return nil, taskmodel.ErrTaskRunAlreadyQueued
		}
	}
	runs = append(runs, r)

	// save manual runs
	runsBytes, err := json.Marshal(runs)
	if err != nil {
		return nil, taskmodel.ErrInternalTaskServiceError(err)
	}

	key := taskManualRunKey(taskID)

	if err := bucket.Put(key, runsBytes); err != nil {
		return nil, taskmodel.ErrUnexpectedTaskBucketErr(err)
	}

	return r, nil
}

// CreateRun creates a run with a scheduledFor time as now.
func (s *Service) CreateRun(ctx context.Context, taskID platform.ID, scheduledFor time.Time, runAt time.Time) (*taskmodel.Run, error) {
	var r *taskmodel.Run
	err := s.kv.Update(func(tx storage.Tx) error {
		run, err := s.createRun(ctx, tx, taskID, scheduledFor, runAt)
		if err != nil {
			return err
		}
		r = run
		return nil
	})
	return r, err
}
func (s *Service) createRun(ctx context.Context, tx storage.Tx, taskID platform.ID, scheduledFor time.Time, runAt time.Time) (*taskmodel.Run, error) {
	id := s.IDGenerator.ID()
	t := time.Unix(scheduledFor.Unix(), 0).UTC()

	run := taskmodel.Run{
		ID:           id,
		TaskID:       taskID,
		ScheduledFor: t,
		RunAt:        runAt,
		Status:       taskmodel.RunScheduled.String(),
		Log:          []taskmodel.Log{},
	}

	b := wrappedTx{
		tx:     tx,
		prefix: taskRunPrefix,
	}

	runBytes, err := json.Marshal(run)
	if err != nil {
		return nil, taskmodel.ErrInternalTaskServiceError(err)
	}

	runKey := taskRunKey(taskID, run.ID)
	if err := b.Put(runKey, runBytes); err != nil {
		return nil, taskmodel.ErrUnexpectedTaskBucketErr(err)
	}

	return &run, nil
}

func (s *Service) CurrentlyRunning(ctx context.Context, taskID platform.ID) ([]*taskmodel.Run, error) {
	var runs []*taskmodel.Run
	err := s.kv.View(func(tx storage.ReadOnlyTx) error {
		rs, err := s.currentlyRunning(ctx, tx, taskID)
		if err != nil {
			return err
		}
		runs = rs
		return nil
	})
	if err != nil {
		return nil, err
	}

	return runs, nil
}

func (s *Service) currentlyRunning(ctx context.Context, tx storage.ReadOnlyTx, taskID platform.ID) ([]*taskmodel.Run, error) {
	bucket := wrappedReadTx{
		tx:     tx,
		prefix: taskRunPrefix,
	}

	kvList, err := bucket.List(taskID.String())
	if err != nil {
		return nil, taskmodel.ErrUnexpectedTaskBucketErr(err)
	}
	var runs []*taskmodel.Run

	for _, v := range kvList {
		if strings.HasSuffix(v.Key, "manualRuns") || strings.HasSuffix(v.Key, "latestCompleted") {
			continue
		}
		r := &taskmodel.Run{}
		if err := json.Unmarshal(v.Value, r); err != nil {
			return nil, taskmodel.ErrInternalTaskServiceError(err)
		}

		runs = append(runs, r)
	}
	return runs, nil
}

func (s *Service) ManualRuns(ctx context.Context, taskID platform.ID) ([]*taskmodel.Run, error) {
	var runs []*taskmodel.Run
	err := s.kv.View(func(tx storage.ReadOnlyTx) error {
		rs, err := s.manualRuns(ctx, tx, taskID)
		if err != nil {
			return err
		}
		runs = rs
		return nil
	})
	if err != nil {
		return nil, err
	}

	return runs, nil
}

func (s *Service) manualRuns(ctx context.Context, tx storage.ReadOnlyTx, taskID platform.ID) ([]*taskmodel.Run, error) {
	b := wrappedReadTx{
		tx:     tx,
		prefix: taskRunPrefix,
	}
	key := taskManualRunKey(taskID)

	runs := make([]*taskmodel.Run, 0)
	val, err := b.Get(key)
	if err != nil {
		if IsNotFound(err) {
			return runs, nil
		}
		return nil, taskmodel.ErrUnexpectedTaskBucketErr(err)
	}
	if err := json.Unmarshal(val.Value, &runs); err != nil {
		return nil, taskmodel.ErrInternalTaskServiceError(err)
	}
	return runs, nil
}

func (s *Service) StartManualRun(ctx context.Context, taskID, runID platform.ID) (*taskmodel.Run, error) {
	var r *taskmodel.Run
	err := s.kv.Update(func(tx storage.Tx) error {
		run, err := s.startManualRun(ctx, tx, taskID, runID)
		if err != nil {
			return err
		}
		r = run
		return nil
	})
	return r, err
}

func (s *Service) startManualRun(ctx context.Context, tx storage.Tx, taskID, runID platform.ID) (*taskmodel.Run, error) {

	mRuns, err := s.manualRuns(ctx, tx, taskID)
	if err != nil {
		return nil, taskmodel.ErrRunNotFound
	}

	if len(mRuns) < 1 {
		return nil, taskmodel.ErrRunNotFound
	}

	var run *taskmodel.Run
	for i, r := range mRuns {
		if r.ID == runID {
			run = r
			mRuns = append(mRuns[:i], mRuns[i+1:]...)
		}
	}
	if run == nil {
		return nil, taskmodel.ErrRunNotFound
	}

	// save manual runs
	mRunsBytes, err := json.Marshal(mRuns)
	if err != nil {
		return nil, taskmodel.ErrInternalTaskServiceError(err)
	}

	runsKey := taskManualRunKey(taskID)

	b := &wrappedTx{
		tx:     tx,
		prefix: taskRunPrefix,
	}

	if err := b.Put(runsKey, mRunsBytes); err != nil {
		return nil, taskmodel.ErrUnexpectedTaskBucketErr(err)
	}

	// add mRun to the list of currently running
	mRunBytes, err := json.Marshal(run)
	if err != nil {
		return nil, taskmodel.ErrInternalTaskServiceError(err)
	}

	runKey := taskRunKey(taskID, run.ID)

	if err := b.Put(runKey, mRunBytes); err != nil {
		return nil, taskmodel.ErrUnexpectedTaskBucketErr(err)
	}

	return run, nil
}

// FinishRun removes runID from the list of running tasks and if its `now` is later then last completed update it.
func (s *Service) FinishRun(ctx context.Context, taskID, runID platform.ID) (*taskmodel.Run, error) {
	var run *taskmodel.Run
	err := s.kv.Update(func(tx storage.Tx) error {
		r, err := s.finishRun(ctx, tx, taskID, runID)
		if err != nil {
			return err
		}
		run = r
		return nil
	})
	return run, err
}

func (s *Service) finishRun(ctx context.Context, tx storage.Tx, taskID, runID platform.ID) (*taskmodel.Run, error) {
	// get the run
	r, err := s.findRunByID(ctx, tx, taskID, runID)
	if err != nil {
		return nil, err
	}

	// tell task to update latest completed
	scheduled := r.ScheduledFor

	var latestSuccess, latestFailure *time.Time

	if r.Status == "failed" {
		latestFailure = &scheduled
	} else {
		latestSuccess = &scheduled
	}

	_, err = s.updateTask(ctx, tx, taskID, taskmodel.TaskUpdate{
		LatestCompleted: &scheduled,
		LatestSuccess:   latestSuccess,
		LatestFailure:   latestFailure,
		LastRunStatus:   &r.Status,
		LastRunError: func() *string {
			if r.Status == "failed" {
				// prefer the second to last log message as the error message
				// per https://github.com/influxdata/influxdb/issues/15153#issuecomment-547706005
				if len(r.Log) > 1 {
					return &r.Log[len(r.Log)-2].Message
				} else if len(r.Log) > 0 {
					return &r.Log[len(r.Log)-1].Message
				}
			}
			return nil
		}(),
	})
	if err != nil {
		return nil, err
	}

	// remove run
	bucket := wrappedTx{
		tx:     tx,
		prefix: taskRunPrefix,
	}
	key := taskRunKey(taskID, runID)
	if err := bucket.Delete(key); err != nil {
		return nil, taskmodel.ErrUnexpectedTaskBucketErr(err)
	}

	return r, nil
}

// UpdateRunState sets the run state at the respective time.
func (s *Service) UpdateRunState(ctx context.Context, taskID, runID platform.ID, when time.Time, state taskmodel.RunStatus) error {
	err := s.kv.Update(func(tx storage.Tx) error {
		err := s.updateRunState(ctx, tx, taskID, runID, when, state)
		if err != nil {
			return err
		}
		return nil
	})
	return err
}

func (s *Service) updateRunState(ctx context.Context, tx storage.Tx, taskID, runID platform.ID, when time.Time, state taskmodel.RunStatus) error {
	// find run
	run, err := s.findRunByID(ctx, tx, taskID, runID)
	if err != nil {
		return err
	}

	// update state
	run.Status = state.String()
	switch state {
	case taskmodel.RunStarted:
		run.StartedAt = when
	case taskmodel.RunSuccess, taskmodel.RunFail, taskmodel.RunCanceled:
		run.FinishedAt = when
	}

	// save run
	b := wrappedTx{
		tx:     tx,
		prefix: taskRunPrefix,
	}

	runBytes, err := json.Marshal(run)
	if err != nil {
		return taskmodel.ErrInternalTaskServiceError(err)
	}

	runKey := taskRunKey(taskID, run.ID)
	if err := b.Put(runKey, runBytes); err != nil {
		return taskmodel.ErrUnexpectedTaskBucketErr(err)
	}

	return nil
}

// AddRunLog adds a log line to the run.
func (s *Service) AddRunLog(ctx context.Context, taskID, runID platform.ID, when time.Time, log string) error {
	err := s.kv.Update(func(tx storage.Tx) error {
		err := s.addRunLog(ctx, tx, taskID, runID, when, log)
		if err != nil {
			return err
		}
		return nil
	})
	return err
}

func (s *Service) addRunLog(ctx context.Context, tx storage.Tx, taskID, runID platform.ID, when time.Time, log string) error {
	// find run
	run, err := s.findRunByID(ctx, tx, taskID, runID)
	if err != nil {
		return err
	}
	// update log
	l := taskmodel.Log{RunID: runID, Time: when.Format(time.RFC3339Nano), Message: log}
	run.Log = append(run.Log, l)
	// save run
	b := wrappedTx{
		tx:     tx,
		prefix: taskRunPrefix,
	}

	runBytes, err := json.Marshal(run)
	if err != nil {
		return taskmodel.ErrInternalTaskServiceError(err)
	}

	runKey := taskRunKey(taskID, run.ID)
	if err != nil {
		return err
	}

	if err := b.Put(runKey, runBytes); err != nil {
		return taskmodel.ErrUnexpectedTaskBucketErr(err)
	}

	return nil
}

func taskLatestCompletedKey(taskID platform.ID) string {
	return taskID.String() + "/latestCompleted"
}

func taskManualRunKey(taskID platform.ID) string {
	return taskID.String() + "/manualRuns"
}

func taskRunKey(taskID, runID platform.ID) string {
	return taskID.String() + "/" + runID.String()
}
