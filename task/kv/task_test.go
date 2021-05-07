package kv_test

import (
	"context"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/kapacitor/services/storage/storagetest"
	"github.com/influxdata/kapacitor/task/kv"
	"github.com/influxdata/kapacitor/task/options"
	"github.com/influxdata/kapacitor/task/servicetest"
	"github.com/influxdata/kapacitor/task/taskmodel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKvTaskService(t *testing.T) {
	servicetest.TestTaskService(
		t,
		func(t *testing.T) (*servicetest.System, context.CancelFunc) {
			service := kv.New(storagetest.New())
			service.Open()
			ctx, cancelFunc := context.WithCancel(context.Background())

			go func() {
				<-ctx.Done()
				service.Close()
			}()

			return &servicetest.System{
				TaskControlService: service,
				TaskService:        service,
				Ctx:                ctx,
			}, cancelFunc
		},
		"transactional",
	)
}

type testService struct {
	Service *kv.Service
	Clock   clock.Clock
}

func (s *testService) Close() {
	s.Service.Close()
}

func newService(t *testing.T, ctx context.Context, c clock.Clock) *testService {
	t.Helper()

	if c == nil {
		c = clock.New()
	}

	service := kv.New(storagetest.New(), kv.WithClock(c))
	service.Open()

	return &testService{
		Service: service,
		Clock:   c,
	}
}

func TestService_UpdateTask_InactiveToActive(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	user := t.Name() + "-user"

	c := clock.NewMock()
	c.Set(time.Unix(1000, 0))

	ts := newService(t, ctx, c)
	defer ts.Close()

	originalTask, err := ts.Service.CreateTask(ctx, taskmodel.TaskCreate{
		Flux:          `option task = {name: "a task",every: 1h} from(bucket:"test") |> range(start:-1h)`,
		OwnerUsername: user,
		Status:        string(taskmodel.TaskActive),
	})
	if err != nil {
		t.Fatal("CreateTask", err)
	}

	v := taskmodel.TaskStatusInactive
	c.Add(1 * time.Second)
	exp := c.Now()
	updatedTask, err := ts.Service.UpdateTask(ctx, originalTask.ID, taskmodel.TaskUpdate{Status: &v, LatestCompleted: &exp, LatestScheduled: &exp})
	if err != nil {
		t.Fatal("UpdateTask", err)
	}

	if got := updatedTask.LatestScheduled; !got.Equal(exp) {
		t.Fatalf("unexpected -got/+exp\n%s", cmp.Diff(got.String(), exp.String()))
	}
	if got := updatedTask.LatestCompleted; !got.Equal(exp) {
		t.Fatalf("unexpected -got/+exp\n%s", cmp.Diff(got.String(), exp.String()))
	}

	c.Add(10 * time.Second)
	exp = c.Now()
	v = taskmodel.TaskStatusActive
	updatedTask, err = ts.Service.UpdateTask(ctx, originalTask.ID, taskmodel.TaskUpdate{Status: &v})
	if err != nil {
		t.Fatal("UpdateTask", err)
	}

	if got := updatedTask.LatestScheduled; !got.Equal(exp) {
		t.Fatalf("unexpected -got/+exp\n%s", cmp.Diff(got.String(), exp.String()))
	}
}

func TestTaskRunCancellation(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	user := t.Name() + "-user"

	ts := newService(t, ctx, nil)
	defer ts.Close()

	task, err := ts.Service.CreateTask(ctx, taskmodel.TaskCreate{
		Flux:          `option task = {name: "a task",cron: "0 * * * *", offset: 20s} from(bucket:"test") |> range(start:-1h)`,
		OwnerUsername: user,
	})
	if err != nil {
		t.Fatal(err)
	}

	run, err := ts.Service.CreateRun(ctx, task.ID, time.Now().Add(time.Hour), time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}

	if err := ts.Service.CancelRun(ctx, run.TaskID, run.ID); err != nil {
		t.Fatal(err)
	}

	canceled, err := ts.Service.FindRunByID(ctx, run.TaskID, run.ID)
	if err != nil {
		t.Fatal(err)
	}

	if canceled.Status != taskmodel.RunCanceled.String() {
		t.Fatalf("expected task run to be cancelled")
	}
}

func TestService_UpdateTask_RecordLatestSuccessAndFailure(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	user := t.Name() + "-user"

	c := clock.NewMock()
	c.Set(time.Unix(1000, 0))

	ts := newService(t, ctx, c)
	defer ts.Close()

	originalTask, err := ts.Service.CreateTask(ctx, taskmodel.TaskCreate{
		Flux:          `option task = {name: "a task",every: 1h} from(bucket:"test") |> range(start:-1h)`,
		OwnerUsername: user,
		Status:        string(taskmodel.TaskActive),
	})
	if err != nil {
		t.Fatal("CreateTask", err)
	}

	c.Add(1 * time.Second)
	exp := c.Now()
	updatedTask, err := ts.Service.UpdateTask(ctx, originalTask.ID, taskmodel.TaskUpdate{
		LatestCompleted: &exp,
		LatestScheduled: &exp,

		// These would be updated in a mutually exclusive manner, but we'll set
		// them both to demonstrate that they do change.
		LatestSuccess: &exp,
		LatestFailure: &exp,
	})
	if err != nil {
		t.Fatal("UpdateTask", err)
	}

	if got := updatedTask.LatestScheduled; !got.Equal(exp) {
		t.Fatalf("unexpected -got/+exp\n%s", cmp.Diff(got.String(), exp.String()))
	}
	if got := updatedTask.LatestCompleted; !got.Equal(exp) {
		t.Fatalf("unexpected -got/+exp\n%s", cmp.Diff(got.String(), exp.String()))
	}
	if got := updatedTask.LatestSuccess; !got.Equal(exp) {
		t.Fatalf("unexpected -got/+exp\n%s", cmp.Diff(got.String(), exp.String()))
	}
	if got := updatedTask.LatestFailure; !got.Equal(exp) {
		t.Fatalf("unexpected -got/+exp\n%s", cmp.Diff(got.String(), exp.String()))
	}

	c.Add(5 * time.Second)
	exp = c.Now()
	updatedTask, err = ts.Service.UpdateTask(ctx, originalTask.ID, taskmodel.TaskUpdate{
		LatestCompleted: &exp,
		LatestScheduled: &exp,

		// These would be updated in a mutually exclusive manner, but we'll set
		// them both to demonstrate that they do change.
		LatestSuccess: &exp,
		LatestFailure: &exp,
	})
	if err != nil {
		t.Fatal("UpdateTask", err)
	}

	if got := updatedTask.LatestScheduled; !got.Equal(exp) {
		t.Fatalf("unexpected -got/+exp\n%s", cmp.Diff(got.String(), exp.String()))
	}
	if got := updatedTask.LatestCompleted; !got.Equal(exp) {
		t.Fatalf("unexpected -got/+exp\n%s", cmp.Diff(got.String(), exp.String()))
	}
	if got := updatedTask.LatestSuccess; !got.Equal(exp) {
		t.Fatalf("unexpected -got/+exp\n%s", cmp.Diff(got.String(), exp.String()))
	}
	if got := updatedTask.LatestFailure; !got.Equal(exp) {
		t.Fatalf("unexpected -got/+exp\n%s", cmp.Diff(got.String(), exp.String()))
	}
}

type taskOptions struct {
	name        string
	every       string
	cron        string
	offset      string
	concurrency int64
	retry       int64
}

func TestExtractTaskOptions(t *testing.T) {
	tcs := []struct {
		name     string
		flux     string
		expected taskOptions
		errMsg   string
	}{
		{
			name: "all parameters",
			flux: `option task = {name: "whatever", every: 1s, offset: 0s, concurrency: 2, retry: 2}`,
			expected: taskOptions{
				name:        "whatever",
				every:       "1s",
				offset:      "0s",
				concurrency: 2,
				retry:       2,
			},
		},
		{
			name: "some extra whitespace and bad content around it",
			flux: `howdy()
			option     task    =     { name:"whatever",  cron:  "* * * * *"  }
			hello()
			`,
			expected: taskOptions{
				name:        "whatever",
				cron:        "* * * * *",
				concurrency: 1,
				retry:       1,
			},
		},
		{
			name:   "bad options",
			flux:   `option task = {name: "whatever", every: 1s, cron: "* * * * *"}`,
			errMsg: "cannot use both cron and every in task options",
		},
		{
			name:   "no options",
			flux:   `doesntexist()`,
			errMsg: "no task options defined",
		},
		{
			name: "multiple assignments",
			flux: `
			option task = {name: "whatever", every: 1s, offset: 0s, concurrency: 2, retry: 2}
			option task = {name: "whatever", every: 1s, offset: 0s, concurrency: 2, retry: 2}
			`,
			errMsg: "multiple task options defined",
		},
		{
			name: "with script calling tableFind",
			flux: `
			import "http"
			import "json"
			option task = {name: "Slack Metrics to #Community", cron: "0 9 * * 5"}
			all_slack_messages = from(bucket: "metrics")
				|> range(start: -7d, stop: now())
				|> filter(fn: (r) =>
					(r._measurement == "slack_channel_message"))
			total_messages = all_slack_messages
				|> group()
				|> count()
				|> tableFind(fn: (key) => true)
			all_slack_messages |> yield()
			`,
			expected: taskOptions{
				name:        "Slack Metrics to #Community",
				cron:        "0 9 * * 5",
				concurrency: 1,
				retry:       1,
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {

			opts, err := options.FromScriptAST(tc.flux)
			if tc.errMsg != "" {
				require.Error(t, err)
				assert.Equal(t, tc.errMsg, err.Error())
				return
			}

			require.NoError(t, err)

			var offset options.Duration
			if opts.Offset != nil {
				offset = *opts.Offset
			}

			var concur int64
			if opts.Concurrency != nil {
				concur = *opts.Concurrency
			}

			var retry int64
			if opts.Retry != nil {
				retry = *opts.Retry
			}

			assert.Equal(t, tc.expected.name, opts.Name)
			assert.Equal(t, tc.expected.cron, opts.Cron)
			assert.Equal(t, tc.expected.every, opts.Every.String())
			assert.Equal(t, tc.expected.offset, offset.String())
			assert.Equal(t, tc.expected.concurrency, concur)
			assert.Equal(t, tc.expected.retry, retry)
		})
	}
}
