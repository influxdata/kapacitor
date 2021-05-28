package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/influxdata/httprouter"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	errors2 "github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	http2 "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/pkg/httpc"
	"github.com/influxdata/kapacitor/auth"
	"github.com/influxdata/kapacitor/services/httpd"
	"github.com/influxdata/kapacitor/task/options"
	"github.com/influxdata/kapacitor/task/taskmodel"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// TaskHandler represents an HTTP API handler for tasks.
type TaskHandler struct {
	log         *zap.Logger
	TaskService taskmodel.TaskService
	errors2.HTTPErrorHandler
	router *httprouter.Router
}

const (
	prefixKapacitor        = "/kapacitor/v1"
	prefixTasks            = "/api/v2/tasks"
	prefixKapacitorTasks   = prefixKapacitor + prefixTasks
	tasksIDPath            = prefixTasks + "/:id"
	tasksIDLogsPath        = prefixTasks + "/:id/logs"
	tasksIDRunsPath        = prefixTasks + "/:id/runs"
	tasksIDRunsIDPath      = prefixTasks + "/:id/runs/:rid"
	tasksIDRunsIDLogsPath  = prefixTasks + "/:id/runs/:rid/logs"
	tasksIDRunsIDRetryPath = prefixTasks + "/:id/runs/:rid/retry"
)

func (h *TaskHandler) Handle(w http.ResponseWriter, r *http.Request, user auth.User) {
	req := r.WithContext(WithKapacitorUser(r.Context(), user))
	h.router.ServeHTTP(w, req)
}

// panic handles panics recovered from http handlers.
// It returns a json response with http status code 500 and the recovered error message.
func (h *TaskHandler) panicHandler(w http.ResponseWriter, r *http.Request, rcv interface{}) {
	ctx := r.Context()
	pe := &errors2.Error{
		Code: errors2.EInternal,
		Msg:  "a panic has occurred",
		Err:  fmt.Errorf("%s: %v", r.URL.String(), rcv),
	}

	if entry := h.log.Check(zapcore.ErrorLevel, pe.Msg); entry != nil {
		entry.Stack = string(debug.Stack())
		entry.Write(zap.Error(pe.Err))
	}

	h.HandleHTTPError(ctx, pe, w)
}

// panic handles panics recovered from http handlers.
// It returns a json response with http status code 500 and the recovered error message.
func (h *TaskHandler) notFoundHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	pe := &errors2.Error{
		Code: errors2.ENotFound,
		Msg:  "url not found",
		Err:  fmt.Errorf("%s: not found", r.URL.String()),
	}
	h.HandleHTTPError(ctx, pe, w)
}

type contextKey string

var userContextKey contextKey = "user"

func WithKapacitorUser(ctx context.Context, user auth.User) context.Context {
	return context.WithValue(ctx, userContextKey, user)
}

func KapacitorUserFromContext(ctx context.Context) *auth.User {
	user, ok := ctx.Value(userContextKey).(auth.User)
	if !ok {
		return nil
	}
	return &user
}

func (h *TaskHandler) HandlerFunc(method string, pattern string, handler func(w http.ResponseWriter, r *http.Request)) {
	h.router.HandlerFunc(method, prefixKapacitor+pattern, handler)
}

func AddTaskServiceRoutes(handler *httpd.Handler, log *zap.Logger, svc taskmodel.TaskService) error {
	h := &TaskHandler{
		log:              log,
		TaskService:      svc,
		HTTPErrorHandler: http2.ErrorHandler(0),
		router:           httprouter.New(),
	}
	h.router.AddMatchedRouteToContext = true
	h.router.PanicHandler = h.panicHandler
	h.router.NotFound = http.HandlerFunc(h.notFoundHandler)

	h.HandlerFunc("GET", prefixTasks, h.handleGetTasks)
	h.HandlerFunc("POST", prefixTasks, h.handlePostTask)
	h.HandlerFunc("GET", tasksIDPath, h.handleGetTask)
	h.HandlerFunc("PATCH", tasksIDPath, h.handleUpdateTask)
	h.HandlerFunc("DELETE", tasksIDPath, h.handleDeleteTask)
	h.HandlerFunc("GET", tasksIDLogsPath, h.handleGetLogs)
	h.HandlerFunc("GET", tasksIDRunsIDLogsPath, h.handleGetLogs)

	h.HandlerFunc("GET", tasksIDRunsPath, h.handleGetRuns)
	h.HandlerFunc("POST", tasksIDRunsPath, h.handleForceRun)
	h.HandlerFunc("GET", tasksIDRunsIDPath, h.handleGetRun)
	h.HandlerFunc("POST", tasksIDRunsIDRetryPath, h.handleRetryRun)
	h.HandlerFunc("DELETE", tasksIDRunsIDPath, h.handleCancelRun)

	routes := make([]httpd.Route, 0, 10)
	for _, method := range []string{"GET", "POST", "PUT", "PATCH", "DELETE"} {
		for _, prefix := range []string{prefixTasks, prefixTasks + "/"} {
			routes = append(routes, httpd.Route{
				Method:      method,
				Pattern:     prefix,
				HandlerFunc: h.Handle,
				NoJSON:      true, // don't default to json headers (e.g. for DELETE which is 204)
				NoGzip:      true, // don't default to gzip, it has problems with openapi codegen and empty responses
			})
		}
	}
	return handler.AddRoutes(routes)
}

func RemoveTaskServiceRoutes(handler *httpd.Handler) error {
	routes := make([]httpd.Route, 0, 10)
	for _, method := range []string{"GET", "POST", "PUT", "PATCH", "DELETE"} {
		for _, prefix := range []string{prefixTasks, prefixTasks + "/"} {
			routes = append(routes, httpd.Route{
				Method:  method,
				Pattern: prefix,
			})
		}
	}
	handler.DelRoutes(routes)
	return nil
}

// Task is a package-specific Task format that preserves the expected format for the API,
// where time values are represented as strings
type Task struct {
	ID              platform.ID            `json:"id"`
	OwnerUsername   string                 `json:"ownerID"`
	UnusedOrgId     string                 `json:"orgID"`
	Name            string                 `json:"name"`
	Description     string                 `json:"description,omitempty"`
	Status          string                 `json:"status"`
	Flux            string                 `json:"flux"`
	Every           string                 `json:"every,omitempty"`
	Cron            string                 `json:"cron,omitempty"`
	Offset          string                 `json:"offset,omitempty"`
	LatestCompleted string                 `json:"latestCompleted,omitempty"`
	LastRunStatus   string                 `json:"lastRunStatus,omitempty"`
	LastRunError    string                 `json:"lastRunError,omitempty"`
	CreatedAt       string                 `json:"createdAt,omitempty"`
	UpdatedAt       string                 `json:"updatedAt,omitempty"`
	Metadata        map[string]interface{} `json:"metadata,omitempty"`
}

type taskResponse struct {
	Links  map[string]string `json:"links"`
	Labels []influxdb.Label  `json:"labels"`
	Task
}

func encodeResponse(ctx context.Context, w http.ResponseWriter, code int, res interface{}) error {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)

	return json.NewEncoder(w).Encode(res)
}

func logEncodingError(log *zap.Logger, r *http.Request, err error) {
	// If we encounter an error while encoding the response to an http request
	// the best thing we can do is log that error, as we may have already written
	// the headers for the http request in question.
	log.Error("Error encoding response",
		zap.String("path", r.URL.Path),
		zap.String("method", r.Method),
		zap.Error(err))
}

// NewFrontEndTask converts a internal task type to a task that we want to display to users
func NewFrontEndTask(t taskmodel.Task) Task {
	latestCompleted := ""
	if !t.LatestCompleted.IsZero() {
		latestCompleted = t.LatestCompleted.Format(time.RFC3339)
	}
	createdAt := ""
	if !t.CreatedAt.IsZero() {
		createdAt = t.CreatedAt.Format(time.RFC3339)
	}
	updatedAt := ""
	if !t.UpdatedAt.IsZero() {
		updatedAt = t.UpdatedAt.Format(time.RFC3339)
	}
	offset := ""
	if t.Offset != 0*time.Second {
		offset = customParseDuration(t.Offset)
	}

	return Task{
		ID:              t.ID,
		OwnerUsername:   t.OwnerUsername,
		Name:            t.Name,
		Description:     t.Description,
		Status:          t.Status,
		Flux:            t.Flux,
		Every:           t.Every,
		Cron:            t.Cron,
		Offset:          offset,
		LatestCompleted: latestCompleted,
		LastRunStatus:   t.LastRunStatus,
		LastRunError:    t.LastRunError,
		CreatedAt:       createdAt,
		UpdatedAt:       updatedAt,
		Metadata:        t.Metadata,
	}
}

func convertTask(t Task) *taskmodel.Task {
	var (
		latestCompleted time.Time
		createdAt       time.Time
		updatedAt       time.Time
		offset          time.Duration
	)

	if t.LatestCompleted != "" {
		latestCompleted, _ = time.Parse(time.RFC3339, t.LatestCompleted)
	}

	if t.CreatedAt != "" {
		createdAt, _ = time.Parse(time.RFC3339, t.CreatedAt)
	}

	if t.UpdatedAt != "" {
		updatedAt, _ = time.Parse(time.RFC3339, t.UpdatedAt)
	}

	if t.Offset != "" {
		var duration options.Duration
		if err := duration.Parse(t.Offset); err == nil {
			offset, _ = duration.DurationFrom(time.Now())
		}
	}

	return &taskmodel.Task{
		ID:              t.ID,
		OwnerUsername:   t.OwnerUsername,
		Name:            t.Name,
		Description:     t.Description,
		Status:          t.Status,
		Flux:            t.Flux,
		Every:           t.Every,
		Cron:            t.Cron,
		Offset:          offset,
		LatestCompleted: latestCompleted,
		LastRunStatus:   t.LastRunStatus,
		LastRunError:    t.LastRunError,
		CreatedAt:       createdAt,
		UpdatedAt:       updatedAt,
		Metadata:        t.Metadata,
	}
}

func customParseDuration(d time.Duration) string {
	str := ""
	if d < 0 {
		str = "-"
		d = d * -1
	}

	// parse hours
	hours := d / time.Hour
	if hours != 0 {
		str = fmt.Sprintf("%s%dh", str, hours)
	}
	if d%time.Hour == 0 {
		return str
	}
	// parse minutes
	d = d - (time.Duration(hours) * time.Hour)

	min := d / time.Minute
	if min != 0 {
		str = fmt.Sprintf("%s%dm", str, min)
	}
	if d%time.Minute == 0 {
		return str
	}

	// parse seconds
	d = d - time.Duration(min)*time.Minute
	sec := d / time.Second

	if sec != 0 {
		str = fmt.Sprintf("%s%ds", str, sec)
	}
	return str
}

func newTaskResponse(t taskmodel.Task) taskResponse {
	response := taskResponse{
		Links: map[string]string{
			"self": fmt.Sprintf(prefixKapacitorTasks+"/%s", t.ID),
			"runs": fmt.Sprintf(prefixKapacitorTasks+"/%s/runs", t.ID),
			"logs": fmt.Sprintf(prefixKapacitorTasks+"/%s/logs", t.ID),
		},
		Task: NewFrontEndTask(t),
	}

	return response
}

func newTasksPagingLinks(basePath string, ts []*taskmodel.Task, f taskmodel.TaskFilter) *influxdb.PagingLinks {
	var self, next string
	u := url.URL{
		Path: basePath,
	}

	values := url.Values{}
	for k, vs := range f.QueryParams() {
		for _, v := range vs {
			if v != "" {
				values.Add(k, v)
			}
		}
	}

	u.RawQuery = values.Encode()
	self = u.String()

	if len(ts) >= f.Limit {
		values.Set("after", ts[f.Limit-1].ID.String())
		u.RawQuery = values.Encode()
		next = u.String()
	}

	links := &influxdb.PagingLinks{
		Self: self,
		Next: next,
	}

	return links
}

type tasksResponse struct {
	Links *influxdb.PagingLinks `json:"links"`
	Tasks []taskResponse        `json:"tasks"`
}

func newTasksResponse(ctx context.Context, ts []*taskmodel.Task, f taskmodel.TaskFilter) tasksResponse {
	rs := tasksResponse{
		Links: newTasksPagingLinks(prefixKapacitorTasks, ts, f),
		Tasks: make([]taskResponse, len(ts)),
	}
	for i := range ts {
		rs.Tasks[i] = newTaskResponse(*ts[i])
	}
	return rs
}

type runResponse struct {
	Links map[string]string `json:"links,omitempty"`
	httpRun
}

// httpRun is a version of the Run object used to communicate over the API
// it uses a pointer to a time.Time instead of a time.Time so that we can pass a nil
// value for empty time values
type httpRun struct {
	ID           platform.ID     `json:"id,omitempty"`
	TaskID       platform.ID     `json:"taskID"`
	Status       string          `json:"status"`
	ScheduledFor *time.Time      `json:"scheduledFor"`
	StartedAt    *time.Time      `json:"startedAt,omitempty"`
	FinishedAt   *time.Time      `json:"finishedAt,omitempty"`
	RequestedAt  *time.Time      `json:"requestedAt,omitempty"`
	Log          []taskmodel.Log `json:"log,omitempty"`
}

func newRunResponse(r taskmodel.Run) runResponse {
	run := httpRun{
		ID:           r.ID,
		TaskID:       r.TaskID,
		Status:       r.Status,
		Log:          r.Log,
		ScheduledFor: &r.ScheduledFor,
	}

	if !r.StartedAt.IsZero() {
		run.StartedAt = &r.StartedAt
	}
	if !r.FinishedAt.IsZero() {
		run.FinishedAt = &r.FinishedAt
	}
	if !r.RequestedAt.IsZero() {
		run.RequestedAt = &r.RequestedAt
	}

	return runResponse{
		Links: map[string]string{
			"self":  fmt.Sprintf(prefixKapacitorTasks+"/%s/runs/%s", r.TaskID, r.ID),
			"task":  fmt.Sprintf(prefixKapacitorTasks+"/%s", r.TaskID),
			"logs":  fmt.Sprintf(prefixKapacitorTasks+"/%s/runs/%s/logs", r.TaskID, r.ID),
			"retry": fmt.Sprintf(prefixKapacitorTasks+"/%s/runs/%s/retry", r.TaskID, r.ID),
		},
		httpRun: run,
	}
}

func convertRun(r httpRun) *taskmodel.Run {
	run := &taskmodel.Run{
		ID:     r.ID,
		TaskID: r.TaskID,
		Status: r.Status,
		Log:    r.Log,
	}

	if r.StartedAt != nil {
		run.StartedAt = *r.StartedAt
	}

	if r.FinishedAt != nil {
		run.FinishedAt = *r.FinishedAt
	}

	if r.RequestedAt != nil {
		run.RequestedAt = *r.RequestedAt
	}

	if r.ScheduledFor != nil {
		run.ScheduledFor = *r.ScheduledFor
	}

	return run
}

type runsResponse struct {
	Links map[string]string `json:"links"`
	Runs  []*runResponse    `json:"runs"`
}

func newRunsResponse(rs []*taskmodel.Run, taskID platform.ID) runsResponse {
	r := runsResponse{
		Links: map[string]string{
			"self": fmt.Sprintf(prefixKapacitorTasks+"/%s/runs", taskID),
			"task": fmt.Sprintf(prefixKapacitorTasks+"/%s", taskID),
		},
		Runs: make([]*runResponse, len(rs)),
	}

	for i := range rs {
		rs := newRunResponse(*rs[i])
		r.Runs[i] = &rs
	}
	return r
}

func (h *TaskHandler) handleGetTasks(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodeGetTasksRequest(ctx, r)
	if err != nil {
		err = &errors2.Error{
			Err:  err,
			Code: errors2.EInvalid,
			Msg:  "failed to decode request",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	tasks, _, err := h.TaskService.FindTasks(ctx, req.filter)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Tasks retrieved", zap.String("tasks", fmt.Sprint(tasks)))
	if err := encodeResponse(ctx, w, http.StatusOK, newTasksResponse(ctx, tasks, req.filter)); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type getTasksRequest struct {
	filter taskmodel.TaskFilter
}

func decodeGetTasksRequest(ctx context.Context, r *http.Request) (*getTasksRequest, error) {
	qp := r.URL.Query()
	req := &getTasksRequest{}

	if after := qp.Get("after"); after != "" {
		id, err := platform.IDFromString(after)
		if err != nil {
			return nil, err
		}
		req.filter.After = id
	}

	if userName := qp.Get("user"); userName != "" {
		req.filter.Username = &userName
	}

	if limit := qp.Get("limit"); limit != "" {
		lim, err := strconv.Atoi(limit)
		if err != nil {
			return nil, err
		}
		if lim < 1 || lim > taskmodel.TaskMaxPageSize {
			return nil, &errors2.Error{
				Code: errors2.EUnprocessableEntity,
				Msg:  fmt.Sprintf("limit must be between 1 and %d", taskmodel.TaskMaxPageSize),
			}
		}
		req.filter.Limit = lim
	} else {
		req.filter.Limit = taskmodel.TaskDefaultPageSize
	}

	if status := qp.Get("status"); status == "active" {
		req.filter.Status = &status
	} else if status := qp.Get("status"); status == "inactive" {
		req.filter.Status = &status
	}

	// the task api can only create or lookup system tasks.
	req.filter.Type = &taskmodel.TaskSystemType

	if name := qp.Get("name"); name != "" {
		req.filter.Name = &name
	}

	return req, nil
}

func (h *TaskHandler) handlePostTask(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodePostTaskRequest(ctx, r)
	if err != nil {
		err = &errors2.Error{
			Err:  err,
			Code: errors2.EInvalid,
			Msg:  "failed to decode request",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	task, err := h.TaskService.CreateTask(ctx, req.TaskCreate)
	if err != nil {
		// if the error is not already a errors2.Error then make it into one
		if _, ok := err.(*errors2.Error); !ok {
			err = &errors2.Error{
				Err:  err,
				Code: errors2.EInternal,
				Msg:  "failed to create task",
			}
		}

		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusCreated, newTaskResponse(*task)); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type postTaskRequest struct {
	TaskCreate taskmodel.TaskCreate
}

func decodePostTaskRequest(ctx context.Context, r *http.Request) (*postTaskRequest, error) {
	var tc taskmodel.TaskCreate
	if err := json.NewDecoder(r.Body).Decode(&tc); err != nil {
		return nil, err
	}

	if user := KapacitorUserFromContext(r.Context()); user != nil && user.Name() != "" {
		tc.OwnerUsername = user.Name()
	}
	tc.Type = taskmodel.TaskSystemType

	if err := tc.Validate(); err != nil {
		return nil, err
	}

	return &postTaskRequest{
		TaskCreate: tc,
	}, nil
}

func (h *TaskHandler) handleGetTask(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodeGetTaskRequest(ctx, r)
	if err != nil {
		err = &errors2.Error{
			Err:  err,
			Code: errors2.EInvalid,
			Msg:  "failed to decode request",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	task, err := h.TaskService.FindTaskByID(ctx, req.TaskID)
	if err != nil {
		err = &errors2.Error{
			Err:  err,
			Code: errors2.ENotFound,
			Msg:  "failed to find task",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err != nil {
		err = &errors2.Error{
			Err: err,
			Msg: "failed to find resource labels",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Task retrieved", zap.String("tasks", fmt.Sprint(task)))
	if err := encodeResponse(ctx, w, http.StatusOK, newTaskResponse(*task)); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type getTaskRequest struct {
	TaskID platform.ID
}

func decodeGetTaskRequest(ctx context.Context, r *http.Request) (*getTaskRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &errors2.Error{
			Code: errors2.EInvalid,
			Msg:  "url missing id",
		}
	}

	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}

	req := &getTaskRequest{
		TaskID: i,
	}

	return req, nil
}

func (h *TaskHandler) handleUpdateTask(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodeUpdateTaskRequest(ctx, r)
	if err != nil {
		err = &errors2.Error{
			Err:  err,
			Code: errors2.EInvalid,
			Msg:  "failed to decode request",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}
	task, err := h.TaskService.UpdateTask(ctx, req.TaskID, req.Update)
	if err != nil {
		err := &errors2.Error{
			Err: err,
			Msg: "failed to update task",
		}
		if err.Err == taskmodel.ErrTaskNotFound {
			err.Code = errors2.ENotFound
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err != nil {
		err = &errors2.Error{
			Err: err,
			Msg: "failed to find resource labels",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Tasks updated", zap.String("task", fmt.Sprint(task)))
	if err := encodeResponse(ctx, w, http.StatusOK, newTaskResponse(*task)); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type updateTaskRequest struct {
	Update taskmodel.TaskUpdate
	TaskID platform.ID
}

func decodeUpdateTaskRequest(ctx context.Context, r *http.Request) (*updateTaskRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &errors2.Error{
			Code: errors2.EInvalid,
			Msg:  "you must provide a task ID",
		}
	}

	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}

	var upd taskmodel.TaskUpdate
	if err := json.NewDecoder(r.Body).Decode(&upd); err != nil {
		return nil, err
	}

	if err := upd.Validate(); err != nil {
		return nil, err
	}

	return &updateTaskRequest{
		Update: upd,
		TaskID: i,
	}, nil
}

func (h *TaskHandler) handleDeleteTask(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodeDeleteTaskRequest(ctx, r)
	if err != nil {
		err = &errors2.Error{
			Err:  err,
			Code: errors2.EInvalid,
			Msg:  "failed to decode request",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := h.TaskService.DeleteTask(ctx, req.TaskID); err != nil {
		err := &errors2.Error{
			Err: err,
			Msg: "failed to delete task",
		}
		if err.Err == taskmodel.ErrTaskNotFound {
			err.Code = errors2.ENotFound
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Tasks deleted", zap.String("taskID", fmt.Sprint(req.TaskID)))
	w.WriteHeader(http.StatusNoContent)
}

type deleteTaskRequest struct {
	TaskID platform.ID
}

func decodeDeleteTaskRequest(ctx context.Context, r *http.Request) (*deleteTaskRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &errors2.Error{
			Code: errors2.EInvalid,
			Msg:  "you must provide a task ID",
		}
	}

	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}

	return &deleteTaskRequest{
		TaskID: i,
	}, nil
}

func (h *TaskHandler) handleGetLogs(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeGetLogsRequest(ctx, r)
	if err != nil {
		err = &errors2.Error{
			Err:  err,
			Code: errors2.EInvalid,
			Msg:  "failed to decode request",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	logs, _, err := h.TaskService.FindLogs(ctx, req.filter)
	if err != nil {
		err := &errors2.Error{
			Err: err,
			Msg: "failed to find task logs",
		}
		if err.Err == taskmodel.ErrTaskNotFound || err.Err == taskmodel.ErrNoRunsFound {
			err.Code = errors2.ENotFound
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, &getLogsResponse{Events: logs}); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type getLogsRequest struct {
	filter taskmodel.LogFilter
}

type getLogsResponse struct {
	Events []*taskmodel.Log `json:"events"`
}

func decodeGetLogsRequest(ctx context.Context, r *http.Request) (*getLogsRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &errors2.Error{
			Code: errors2.EInvalid,
			Msg:  "you must provide a task ID",
		}
	}

	req := &getLogsRequest{}
	taskID, err := platform.IDFromString(id)
	if err != nil {
		return nil, err
	}
	req.filter.Task = *taskID

	if runID := params.ByName("rid"); runID != "" {
		id, err := platform.IDFromString(runID)
		if err != nil {
			return nil, err
		}
		req.filter.Run = id
	}

	return req, nil
}

func (h *TaskHandler) handleGetRuns(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeGetRunsRequest(ctx, r)
	if err != nil {
		err = &errors2.Error{
			Err:  err,
			Code: errors2.EInvalid,
			Msg:  "failed to decode request",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	runs, _, err := h.TaskService.FindRuns(ctx, req.filter)
	if err != nil {
		err := &errors2.Error{
			Err: err,
			Msg: "failed to find runs",
		}
		if err.Err == taskmodel.ErrTaskNotFound || err.Err == taskmodel.ErrNoRunsFound {
			err.Code = errors2.ENotFound
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, newRunsResponse(runs, req.filter.Task)); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type getRunsRequest struct {
	filter taskmodel.RunFilter
}

func decodeGetRunsRequest(ctx context.Context, r *http.Request) (*getRunsRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &errors2.Error{
			Code: errors2.EInvalid,
			Msg:  "you must provide a task ID",
		}
	}

	req := &getRunsRequest{}
	taskID, err := platform.IDFromString(id)
	if err != nil {
		return nil, err
	}
	req.filter.Task = *taskID

	qp := r.URL.Query()

	if id := qp.Get("after"); id != "" {
		afterID, err := platform.IDFromString(id)
		if err != nil {
			return nil, err
		}
		req.filter.After = afterID
	}

	if limit := qp.Get("limit"); limit != "" {
		i, err := strconv.Atoi(limit)
		if err != nil {
			return nil, err
		}

		if i < 1 || i > taskmodel.TaskMaxPageSize {
			return nil, taskmodel.ErrOutOfBoundsLimit
		}
		req.filter.Limit = i
	}

	var at, bt string
	var afterTime, beforeTime time.Time
	if at = qp.Get("afterTime"); at != "" {
		afterTime, err = time.Parse(time.RFC3339, at)
		if err != nil {
			return nil, err
		}
		req.filter.AfterTime = at
	}

	if bt = qp.Get("beforeTime"); bt != "" {
		beforeTime, err = time.Parse(time.RFC3339, bt)
		if err != nil {
			return nil, err
		}
		req.filter.BeforeTime = bt
	}

	if at != "" && bt != "" && !beforeTime.After(afterTime) {
		return nil, &errors2.Error{
			Code: errors2.EUnprocessableEntity,
			Msg:  "beforeTime must be later than afterTime",
		}
	}

	return req, nil
}

func (h *TaskHandler) handleForceRun(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeForceRunRequest(ctx, r)
	if err != nil {
		err = &errors2.Error{
			Err:  err,
			Code: errors2.EInvalid,
			Msg:  "failed to decode request",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	run, err := h.TaskService.ForceRun(ctx, req.TaskID, req.Timestamp)
	if err != nil {
		err := &errors2.Error{
			Err: err,
			Msg: "failed to force run",
		}
		if err.Err == taskmodel.ErrTaskNotFound {
			err.Code = errors2.ENotFound
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}
	if err := encodeResponse(ctx, w, http.StatusCreated, newRunResponse(*run)); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type forceRunRequest struct {
	TaskID    platform.ID
	Timestamp int64
}

func decodeForceRunRequest(ctx context.Context, r *http.Request) (forceRunRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	tid := params.ByName("id")
	if tid == "" {
		return forceRunRequest{}, &errors2.Error{
			Code: errors2.EInvalid,
			Msg:  "you must provide a task ID",
		}
	}

	var ti platform.ID
	if err := ti.DecodeFromString(tid); err != nil {
		return forceRunRequest{}, err
	}

	var req struct {
		ScheduledFor string `json:"scheduledFor"`
	}

	if r.ContentLength != 0 && r.ContentLength < 1000 { // prevent attempts to use up memory since r.Body should include at most one item (RunManually)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			return forceRunRequest{}, err
		}
	}

	var t time.Time
	if req.ScheduledFor == "" {
		t = time.Now()
	} else {
		var err error
		t, err = time.Parse(time.RFC3339, req.ScheduledFor)
		if err != nil {
			return forceRunRequest{}, err
		}
	}

	return forceRunRequest{
		TaskID:    ti,
		Timestamp: t.Unix(),
	}, nil
}

func (h *TaskHandler) handleGetRun(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeGetRunRequest(ctx, r)
	if err != nil {
		err = &errors2.Error{
			Err:  err,
			Code: errors2.EInvalid,
			Msg:  "failed to decode request",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	run, err := h.TaskService.FindRunByID(ctx, req.TaskID, req.RunID)
	if err != nil {
		err := &errors2.Error{
			Err: err,
			Msg: "failed to find run",
		}
		if err.Err == taskmodel.ErrTaskNotFound || err.Err == taskmodel.ErrRunNotFound {
			err.Code = errors2.ENotFound
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, newRunResponse(*run)); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type getRunRequest struct {
	TaskID platform.ID
	RunID  platform.ID
}

func decodeGetRunRequest(ctx context.Context, r *http.Request) (*getRunRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	tid := params.ByName("id")
	if tid == "" {
		return nil, &errors2.Error{
			Code: errors2.EInvalid,
			Msg:  "you must provide a task ID",
		}
	}
	rid := params.ByName("rid")
	if rid == "" {
		return nil, &errors2.Error{
			Code: errors2.EInvalid,
			Msg:  "you must provide a run ID",
		}
	}

	var ti, ri platform.ID
	if err := ti.DecodeFromString(tid); err != nil {
		return nil, err
	}
	if err := ri.DecodeFromString(rid); err != nil {
		return nil, err
	}

	return &getRunRequest{
		RunID:  ri,
		TaskID: ti,
	}, nil
}

type cancelRunRequest struct {
	RunID  platform.ID
	TaskID platform.ID
}

func decodeCancelRunRequest(ctx context.Context, r *http.Request) (*cancelRunRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	rid := params.ByName("rid")
	if rid == "" {
		return nil, &errors2.Error{
			Code: errors2.EInvalid,
			Msg:  "you must provide a run ID",
		}
	}
	tid := params.ByName("id")
	if tid == "" {
		return nil, &errors2.Error{
			Code: errors2.EInvalid,
			Msg:  "you must provide a task ID",
		}
	}

	var i platform.ID
	if err := i.DecodeFromString(rid); err != nil {
		return nil, err
	}
	var t platform.ID
	if err := t.DecodeFromString(tid); err != nil {
		return nil, err
	}

	return &cancelRunRequest{
		RunID:  i,
		TaskID: t,
	}, nil
}

func (h *TaskHandler) handleCancelRun(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeCancelRunRequest(ctx, r)
	if err != nil {
		err = &errors2.Error{
			Err:  err,
			Code: errors2.EInvalid,
			Msg:  "failed to decode request",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	err = h.TaskService.CancelRun(ctx, req.TaskID, req.RunID)
	if err != nil {
		err := &errors2.Error{
			Err: err,
			Msg: "failed to cancel run",
		}
		if err.Err == taskmodel.ErrTaskNotFound || err.Err == taskmodel.ErrRunNotFound {
			err.Code = errors2.ENotFound
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}
}

func (h *TaskHandler) handleRetryRun(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeRetryRunRequest(ctx, r)
	if err != nil {
		err = &errors2.Error{
			Err:  err,
			Code: errors2.EInvalid,
			Msg:  "failed to decode request",
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	run, err := h.TaskService.RetryRun(ctx, req.TaskID, req.RunID)
	if err != nil {
		err := &errors2.Error{
			Err: err,
			Msg: "failed to retry run",
		}
		if err.Err == taskmodel.ErrTaskNotFound || err.Err == taskmodel.ErrRunNotFound {
			err.Code = errors2.ENotFound
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}
	if err := encodeResponse(ctx, w, http.StatusOK, newRunResponse(*run)); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type retryRunRequest struct {
	RunID, TaskID platform.ID
}

func decodeRetryRunRequest(ctx context.Context, r *http.Request) (*retryRunRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	tid := params.ByName("id")
	if tid == "" {
		return nil, &errors2.Error{
			Code: errors2.EInvalid,
			Msg:  "you must provide a task ID",
		}
	}
	rid := params.ByName("rid")
	if rid == "" {
		return nil, &errors2.Error{
			Code: errors2.EInvalid,
			Msg:  "you must provide a run ID",
		}
	}

	var ti, ri platform.ID
	if err := ti.DecodeFromString(tid); err != nil {
		return nil, err
	}
	if err := ri.DecodeFromString(rid); err != nil {
		return nil, err
	}

	return &retryRunRequest{
		RunID:  ri,
		TaskID: ti,
	}, nil
}

// TaskService connects to Influx via HTTP using tokens to manage tasks.
type TaskService struct {
	Client *httpc.Client
}

// FindTaskByID returns a single task
func (t TaskService) FindTaskByID(ctx context.Context, id platform.ID) (*taskmodel.Task, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var tr taskResponse
	err := t.Client.Get(taskIDPath(id)).DecodeJSON(&tr).Do(ctx)
	if err != nil {
		return nil, err
	}

	return convertTask(tr.Task), nil
}

// FindTasks returns a list of tasks that match a filter (limit 100) and the total count
// of matching tasks.
func (t TaskService) FindTasks(ctx context.Context, filter taskmodel.TaskFilter) ([]*taskmodel.Task, int, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	// slice of 2-capacity string slices for storing parameter key-value pairs
	var params [][2]string

	if filter.After != nil {
		params = append(params, [2]string{"after", filter.After.String()})
	}
	if filter.Username != nil {
		params = append(params, [2]string{"user", *filter.Username})
	}
	if filter.Limit != 0 {
		params = append(params, [2]string{"limit", strconv.Itoa(filter.Limit)})
	}

	if filter.Status != nil {
		params = append(params, [2]string{"status", *filter.Status})
	}

	if filter.Type != nil {
		params = append(params, [2]string{"type", *filter.Type})
	}

	var tr tasksResponse
	err := t.Client.
		Get(prefixTasks).
		QueryParams(params...).
		DecodeJSON(&tr).
		Do(ctx)
	if err != nil {
		return nil, 0, err
	}

	tasks := make([]*taskmodel.Task, len(tr.Tasks))
	for i := range tr.Tasks {
		tasks[i] = convertTask(tr.Tasks[i].Task)
	}
	return tasks, len(tasks), nil
}

// CreateTask creates a new task.
func (t TaskService) CreateTask(ctx context.Context, tc taskmodel.TaskCreate) (*taskmodel.Task, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()
	var tr taskResponse

	err := t.Client.
		PostJSON(tc, prefixTasks).
		DecodeJSON(&tr).
		Do(ctx)
	if err != nil {
		return nil, err
	}

	return convertTask(tr.Task), nil
}

// UpdateTask updates a single task with changeset.
func (t TaskService) UpdateTask(ctx context.Context, id platform.ID, upd taskmodel.TaskUpdate) (*taskmodel.Task, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var tr taskResponse
	err := t.Client.
		PatchJSON(&upd, taskIDPath(id)).
		Do(ctx)
	if err != nil {
		return nil, err
	}

	return convertTask(tr.Task), nil
}

// DeleteTask removes a task by ID and purges all associated data and scheduled runs.
func (t TaskService) DeleteTask(ctx context.Context, id platform.ID) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	return t.Client.
		Delete(taskIDPath(id)).
		Do(ctx)
}

// FindLogs returns logs for a run.
func (t TaskService) FindLogs(ctx context.Context, filter taskmodel.LogFilter) ([]*taskmodel.Log, int, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if !filter.Task.Valid() {
		return nil, 0, errors.New("task ID required")
	}

	var urlPath string
	if filter.Run == nil {
		urlPath = path.Join(taskIDPath(filter.Task), "logs")
	} else {
		urlPath = path.Join(taskIDRunIDPath(filter.Task, *filter.Run), "logs")
	}

	var logs getLogsResponse
	err := t.Client.
		Get(urlPath).
		DecodeJSON(&logs).
		Do(ctx)

	if err != nil {
		return nil, 0, err
	}

	return logs.Events, len(logs.Events), nil
}

// FindRuns returns a list of runs that match a filter and the total count of returned runs.
func (t TaskService) FindRuns(ctx context.Context, filter taskmodel.RunFilter) ([]*taskmodel.Run, int, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var params [][2]string

	if !filter.Task.Valid() {
		return nil, 0, errors.New("task ID required")
	}

	if filter.After != nil {
		params = append(params, [2]string{"after", filter.After.String()})
	}

	if filter.Limit < 0 || filter.Limit > taskmodel.TaskMaxPageSize {
		return nil, 0, taskmodel.ErrOutOfBoundsLimit
	}

	params = append(params, [2]string{"limit", strconv.Itoa(filter.Limit)})

	var rs runsResponse
	err := t.Client.
		Get(taskIDRunsPath(filter.Task)).
		QueryParams(params...).
		DecodeJSON(&rs).
		Do(ctx)
	if err != nil {
		return nil, 0, err
	}

	runs := make([]*taskmodel.Run, len(rs.Runs))
	for i := range rs.Runs {
		runs[i] = convertRun(rs.Runs[i].httpRun)
	}

	return runs, len(runs), nil
}

// FindRunByID returns a single run of a specific task.
func (t TaskService) FindRunByID(ctx context.Context, taskID, runID platform.ID) (*taskmodel.Run, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var rs = &runResponse{}
	err := t.Client.
		Get(taskIDRunIDPath(taskID, runID)).
		DecodeJSON(rs).
		Do(ctx)

	if err != nil {
		if errors2.ErrorCode(err) == errors2.ENotFound {
			// ErrRunNotFound is expected as part of the FindRunByID contract,
			// so return that actual error instead of a different error that looks like it.
			// TODO cleanup backend error implementation
			return nil, taskmodel.ErrRunNotFound
		}

		return nil, err
	}

	return convertRun(rs.httpRun), nil
}

// RetryRun creates and returns a new run (which is a retry of another run).
func (t TaskService) RetryRun(ctx context.Context, taskID, runID platform.ID) (*taskmodel.Run, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var rs runResponse
	err := t.Client.
		Post(nil, path.Join(taskIDRunIDPath(taskID, runID), "retry")).
		DecodeJSON(&rs).
		Do(ctx)

	if err != nil {
		if errors2.ErrorCode(err) == errors2.ENotFound {
			// ErrRunNotFound is expected as part of the RetryRun contract,
			// so return that actual error instead of a different error that looks like it.
			// TODO cleanup backend task error implementation
			return nil, taskmodel.ErrRunNotFound
		}
		// RequestStillQueuedError is also part of the contract.
		if e := taskmodel.ParseRequestStillQueuedError(err.Error()); e != nil {
			return nil, *e
		}

		return nil, err
	}

	return convertRun(rs.httpRun), nil
}

// ForceRun starts a run manually right now.
func (t TaskService) ForceRun(ctx context.Context, taskID platform.ID, scheduledFor int64) (*taskmodel.Run, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	type body struct {
		scheduledFor string
	}
	b := body{scheduledFor: time.Unix(scheduledFor, 0).UTC().Format(time.RFC3339)}

	rs := &runResponse{}
	err := t.Client.
		PostJSON(b, taskIDRunsPath(taskID)).
		DecodeJSON(&rs).
		Do(ctx)

	if err != nil {
		if errors2.ErrorCode(err) == errors2.ENotFound {
			// ErrRunNotFound is expected as part of the RetryRun contract,
			// so return that actual error instead of a different error that looks like it.
			return nil, taskmodel.ErrRunNotFound
		}

		// RequestStillQueuedError is also part of the contract.
		if e := taskmodel.ParseRequestStillQueuedError(err.Error()); e != nil {
			return nil, *e
		}

		return nil, err
	}

	return convertRun(rs.httpRun), nil
}

func cancelPath(taskID, runID platform.ID) string {
	return path.Join(taskID.String(), runID.String())
}

// CancelRun stops a longer running run.
func (t TaskService) CancelRun(ctx context.Context, taskID, runID platform.ID) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	err := t.Client.
		Delete(cancelPath(taskID, runID)).
		Do(ctx)

	if err != nil {
		return err
	}

	return nil
}

func taskIDPath(id platform.ID) string {
	return path.Join(prefixTasks, id.String())
}

func taskIDRunsPath(id platform.ID) string {
	return path.Join(prefixTasks, id.String(), "runs")
}

func taskIDRunIDPath(taskID, runID platform.ID) string {
	return path.Join(prefixTasks, taskID.String(), "runs", runID.String())
}
