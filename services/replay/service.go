package replay

import (
	"archive/zip"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	client "github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/kapacitor"
	kclient "github.com/influxdata/kapacitor/client/v1"
	"github.com/influxdata/kapacitor/clock"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/services/httpd"
	"github.com/influxdata/kapacitor/services/storage"
	"github.com/pkg/errors"
	"github.com/twinj/uuid"
)

const streamEXT = ".srpl"
const batchEXT = ".brpl"

const precision = "n"

const (
	recordingsPath         = "/recordings"
	recordingsPathAnchored = "/recordings/"
	recordStreamPath       = recordingsPath + "/stream"
	recordBatchPath        = recordingsPath + "/batch"
	recordQueryPath        = recordingsPath + "/query"

	replaysPath         = "/replays"
	replaysPathAnchored = "/replays/"
	replayBatchPath     = replaysPath + "/batch"
	replayQueryPath     = replaysPath + "/query"
)

var validID = regexp.MustCompile(`^[-\._\p{L}0-9]+$`)

// Handles recording, starting, and waiting on replays
type Service struct {
	saveDir string

	recordings RecordingDAO
	replays    ReplayDAO

	routes []httpd.Route

	StorageService interface {
		Store(namespace string) storage.Interface
	}
	TaskStore interface {
		Load(id string) (*kapacitor.Task, error)
	}
	HTTPDService interface {
		AddRoutes([]httpd.Route) error
		DelRoutes([]httpd.Route)
	}
	InfluxDBService interface {
		NewDefaultClient() (client.Client, error)
		NewNamedClient(name string) (client.Client, error)
	}
	TaskMaster interface {
		NewFork(name string, dbrps []kapacitor.DBRP, measurements []string) (*kapacitor.Edge, error)
		DelFork(name string)
		New() *kapacitor.TaskMaster
		Stream(name string) (kapacitor.StreamCollector, error)
	}

	logger *log.Logger
}

// Create a new replay master.
func NewService(conf Config, l *log.Logger) *Service {
	return &Service{
		saveDir: conf.Dir,
		logger:  l,
	}
}

// The storage namespace for all recording data.
const recordingNamespace = "recording_store"
const replayNamespace = "replay_store"

func (s *Service) Open() error {
	// Create DAO
	s.recordings = newRecordingKV(s.StorageService.Store(recordingNamespace))
	s.replays = newReplayKV(s.StorageService.Store(replayNamespace))

	err := os.MkdirAll(s.saveDir, 0755)
	if err != nil {
		return err
	}

	err = s.migrate()
	if err != nil {
		return err
	}

	// Mark all running replays or recordings as failed since
	// we are just starting and they cannot possibly be still running
	s.markFailedRecordings()
	s.markFailedReplays()

	// Setup routes
	s.routes = []httpd.Route{
		{
			Name:        "recording",
			Method:      "GET",
			Pattern:     recordingsPathAnchored,
			HandlerFunc: s.handleRecording,
		},
		{
			Name:        "deleteRecording",
			Method:      "DELETE",
			Pattern:     recordingsPathAnchored,
			HandlerFunc: s.handleDeleteRecording,
		},
		{
			Name:        "/recordings/-cors",
			Method:      "OPTIONS",
			Pattern:     recordingsPathAnchored,
			HandlerFunc: httpd.ServeOptions,
		},
		{
			Name:        "listRecordings",
			Method:      "GET",
			Pattern:     recordingsPath,
			HandlerFunc: s.handleListRecordings,
		},
		{
			Name:        "createRecording",
			Method:      "POST",
			Pattern:     recordStreamPath,
			HandlerFunc: s.handleRecordStream,
		},
		{
			Name:        "createRecording",
			Method:      "POST",
			Pattern:     recordBatchPath,
			HandlerFunc: s.handleRecordBatch,
		},
		{
			Name:        "createRecording",
			Method:      "POST",
			Pattern:     recordQueryPath,
			HandlerFunc: s.handleRecordQuery,
		},
		{
			Name:        "replay",
			Method:      "GET",
			Pattern:     replaysPathAnchored,
			HandlerFunc: s.handleReplay,
		},
		{
			Name:        "deleteReplay",
			Method:      "DELETE",
			Pattern:     replaysPathAnchored,
			HandlerFunc: s.handleDeleteReplay,
		},
		{
			Name:        "/replays/-cors",
			Method:      "OPTIONS",
			Pattern:     replaysPathAnchored,
			HandlerFunc: httpd.ServeOptions,
		},
		{
			Name:        "listReplays",
			Method:      "GET",
			Pattern:     replaysPath,
			HandlerFunc: s.handleListReplays,
		},
		{
			Name:        "createReplay",
			Method:      "POST",
			Pattern:     replaysPath,
			HandlerFunc: s.handleCreateReplay,
		},
		{
			Name:        "replayBatch",
			Method:      "POST",
			Pattern:     replayBatchPath,
			HandlerFunc: s.handleReplayBatch,
		},
		{
			Name:        "replayQuery",
			Method:      "POST",
			Pattern:     replayQueryPath,
			HandlerFunc: s.handleReplayQuery,
		},
	}

	return s.HTTPDService.AddRoutes(s.routes)
}

func (s *Service) migrate() error {
	// Find all recordings and store their metadata into the new storage service.
	files, err := ioutil.ReadDir(s.saveDir)
	if err != nil {
		return errors.Wrap(err, "migrating recording metadata")
	}
	for _, info := range files {
		if info.IsDir() {
			continue
		}
		name := info.Name()
		i := strings.LastIndex(name, ".")
		ext := name[i:]
		id := name[:i]

		var typ RecordingType
		switch ext {
		case streamEXT:
			typ = StreamRecording
		case batchEXT:
			typ = BatchRecording
		default:
			s.logger.Println("E! unknown file in replay dir", name)
			continue
		}
		recording := Recording{
			ID:       id,
			Type:     typ,
			Size:     info.Size(),
			Date:     info.ModTime().UTC(),
			Status:   Finished,
			Progress: 1.0,
		}
		err = s.recordings.Create(recording)
		if err != nil {
			if err == ErrRecordingExists {
				s.logger.Printf("D! skipping recording %s, metadata already migrated", id)
			} else {
				return errors.Wrap(err, "creating recording metadata")
			}
		} else {
			s.logger.Printf("D! recording %s metadata migrated", id)
		}
	}
	return nil
}

func (s *Service) markFailedRecordings() {
	limit := 100
	offset := 0
	for {
		recordings, err := s.recordings.List("", offset, limit)
		if err != nil {
			s.logger.Println("E! failed to retrieve recordings:", err)
		}
		for _, recording := range recordings {
			if recording.Status == Running {
				recording.Status = Failed
				recording.Error = "unexpected Kapacitor shutdown"
				err := s.recordings.Replace(recording)
				if err != nil {
					s.logger.Println("E! failed to set recording status to failed:", err)
				}
			}
		}
		if len(recordings) != limit {
			break
		}
		offset += limit
	}
}

func (s *Service) markFailedReplays() {
	limit := 100
	offset := 0
	for {
		replays, err := s.replays.List("", offset, limit)
		if err != nil {
			s.logger.Println("E! failed to retrieve replays:", err)
		}
		for _, replay := range replays {
			if replay.Status == Running {
				replay.Status = Failed
				replay.Error = "unexpected Kapacitor shutdown"
				err := s.replays.Replace(replay)
				if err != nil {
					s.logger.Println("E! failed to set replay status to failed:", err)
				}
			}
		}
		if len(replays) != limit {
			break
		}
		offset += limit
	}
}

func (s *Service) Close() error {
	s.HTTPDService.DelRoutes(s.routes)
	return nil
}

const recordingsBasePathAnchored = httpd.BasePath + recordingsPathAnchored

func (s *Service) recordingIDFromPath(path string) (string, error) {
	if len(path) <= len(recordingsBasePathAnchored) {
		return "", errors.New("must specify recording id on path")
	}
	id := path[len(recordingsBasePathAnchored):]
	return id, nil
}
func recordingLink(id string) kclient.Link {
	return kclient.Link{Relation: kclient.Self, Href: path.Join(httpd.BasePath, "recordings", id)}
}

func convertRecording(recording Recording) kclient.Recording {
	var typ kclient.TaskType
	switch recording.Type {
	case StreamRecording:
		typ = kclient.StreamTask
	case BatchRecording:
		typ = kclient.BatchTask
	}
	var status kclient.Status
	switch recording.Status {
	case Failed:
		status = kclient.Failed
	case Running:
		status = kclient.Running
	case Finished:
		status = kclient.Finished
	}
	return kclient.Recording{
		Link:     recordingLink(recording.ID),
		ID:       recording.ID,
		Type:     typ,
		Size:     recording.Size,
		Date:     recording.Date,
		Error:    recording.Error,
		Status:   status,
		Progress: recording.Progress,
	}
}

const replaysBasePathAnchored = httpd.BasePath + replaysPathAnchored

func (s *Service) replayIDFromPath(path string) (string, error) {
	if len(path) <= len(replaysBasePathAnchored) {
		return "", errors.New("must specify replay id on path")
	}
	id := path[len(replaysBasePathAnchored):]
	return id, nil
}
func replayLink(id string) kclient.Link {
	return kclient.Link{Relation: kclient.Self, Href: path.Join(httpd.BasePath, "replays", id)}
}

func convertReplay(replay Replay) kclient.Replay {
	var clk kclient.Clock
	switch replay.Clock {
	case Real:
		clk = kclient.Real
	case Fast:
		clk = kclient.Fast
	}
	var status kclient.Status
	switch replay.Status {
	case Failed:
		status = kclient.Failed
	case Running:
		status = kclient.Running
	case Finished:
		status = kclient.Finished
	}
	return kclient.Replay{
		Link:          replayLink(replay.ID),
		ID:            replay.ID,
		Recording:     replay.RecordingID,
		Task:          replay.TaskID,
		RecordingTime: replay.RecordingTime,
		Clock:         clk,
		Date:          replay.Date,
		Error:         replay.Error,
		Status:        status,
		Progress:      replay.Progress,
	}
}

var allRecordingFields = []string{
	"link",
	"id",
	"type",
	"size",
	"date",
	"error",
	"status",
	"progress",
}

func (s *Service) handleListRecordings(w http.ResponseWriter, r *http.Request) {
	pattern := r.URL.Query().Get("pattern")
	fields := r.URL.Query()["fields"]
	if len(fields) == 0 {
		fields = allRecordingFields
	} else {
		// Always return ID field
		fields = append(fields, "id", "link")
	}

	var err error
	offset := int64(0)
	offsetStr := r.URL.Query().Get("offset")
	if offsetStr != "" {
		offset, err = strconv.ParseInt(offsetStr, 10, 64)
		if err != nil {
			httpd.HttpError(w, fmt.Sprintf("invalid offset parameter %q must be an integer: %s", offsetStr, err), true, http.StatusBadRequest)
		}
	}

	limit := int64(100)
	limitStr := r.URL.Query().Get("limit")
	if limitStr != "" {
		limit, err = strconv.ParseInt(limitStr, 10, 64)
		if err != nil {
			httpd.HttpError(w, fmt.Sprintf("invalid limit parameter %q must be an integer: %s", limitStr, err), true, http.StatusBadRequest)
		}
	}

	recordings, err := s.recordings.List(pattern, int(offset), int(limit))

	rs := make([]map[string]interface{}, len(recordings))
	for i, recording := range recordings {
		rs[i] = make(map[string]interface{}, len(fields))
		for _, field := range fields {
			var value interface{}
			switch field {
			case "id":
				value = recording.ID
			case "link":
				value = recordingLink(recording.ID)
			case "type":
				switch recording.Type {
				case StreamRecording:
					value = kclient.StreamTask
				case BatchRecording:
					value = kclient.BatchTask
				}
			case "size":
				value = recording.Size
			case "date":
				value = recording.Date
			case "error":
				value = recording.Error
			case "status":
				switch recording.Status {
				case Failed:
					value = kclient.Failed
				case Running:
					value = kclient.Running
				case Finished:
					value = kclient.Finished
				}
			case "progress":
				value = recording.Progress
			default:
				httpd.HttpError(w, fmt.Sprintf("unsupported field %q", field), true, http.StatusBadRequest)
				return
			}
			rs[i][field] = value
		}
	}
	type response struct {
		Recordings []map[string]interface{} `json:"recordings"`
	}
	w.Write(httpd.MarshalJSON(response{Recordings: rs}, true))
}

func (s *Service) handleRecording(w http.ResponseWriter, r *http.Request) {
	rid, err := s.recordingIDFromPath(r.URL.Path)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusBadRequest)
		return
	}

	recording, err := s.recordings.Get(rid)
	if err != nil {
		httpd.HttpError(w, "error finding recording: "+err.Error(), true, http.StatusInternalServerError)
		return
	}
	if recording.Status == Running {
		w.WriteHeader(http.StatusAccepted)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	w.Write(httpd.MarshalJSON(convertRecording(recording), true))
}

func (s *Service) handleDeleteRecording(w http.ResponseWriter, r *http.Request) {
	rid, err := s.recordingIDFromPath(r.URL.Path)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusBadRequest)
		return
	}
	recording, err := s.recordings.Get(rid)
	if err == ErrNoRecordingExists {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusInternalServerError)
		return
	}
	err = s.recordings.Delete(rid)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusInternalServerError)
		return
	}
	ds, err := parseDataSourceURL(recording.DataURL)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusInternalServerError)
		return
	}

	err = ds.Remove()
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *Service) dataURLFromID(id, ext string) url.URL {
	return url.URL{
		Scheme: "file",
		Path:   path.Join(s.saveDir, id+ext),
	}
}

func (s *Service) handleRecordStream(w http.ResponseWriter, r *http.Request) {
	var opt kclient.RecordStreamOptions
	dec := json.NewDecoder(r.Body)
	err := dec.Decode(&opt)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusBadRequest)
		return
	}
	if opt.ID == "" {
		opt.ID = uuid.NewV4().String()
	}
	if !validID.MatchString(opt.ID) {
		httpd.HttpError(w, fmt.Sprintf("recording ID must contain only letters, numbers, '-', '.' and '_'. %q", opt.ID), true, http.StatusBadRequest)
		return
	}
	t, err := s.TaskStore.Load(opt.Task)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusNotFound)
		return
	}
	dataUrl := s.dataURLFromID(opt.ID, streamEXT)

	recording := Recording{
		ID:      opt.ID,
		DataURL: dataUrl.String(),
		Type:    StreamRecording,
		Date:    time.Now(),
		Status:  Running,
	}
	err = s.recordings.Create(recording)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusInternalServerError)
		return
	}

	// Spawn routine to perform actual recording.
	go func(recording Recording) {
		ds, _ := parseDataSourceURL(dataUrl.String())
		err := s.doRecordStream(opt.ID, ds, opt.Stop, t.DBRPs, t.Measurements())
		s.updateRecordingResult(recording, ds, err)
	}(recording)

	w.WriteHeader(http.StatusCreated)
	w.Write(httpd.MarshalJSON(convertRecording(recording), true))
}

func (s *Service) handleRecordBatch(w http.ResponseWriter, req *http.Request) {
	var opt kclient.RecordBatchOptions
	dec := json.NewDecoder(req.Body)
	err := dec.Decode(&opt)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusBadRequest)
		return
	}
	if opt.ID == "" {
		opt.ID = uuid.NewV4().String()
	}
	if !validID.MatchString(opt.ID) {
		httpd.HttpError(w, fmt.Sprintf("recording ID must contain only letters, numbers, '-', '.' and '_'. %q", opt.ID), true, http.StatusBadRequest)
		return
	}

	if opt.Start.IsZero() {
		httpd.HttpError(w, "must provide start time", true, http.StatusBadRequest)
		return
	}
	if opt.Stop.IsZero() {
		opt.Stop = time.Now()
	}

	t, err := s.TaskStore.Load(opt.Task)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusNotFound)
		return
	}
	dataUrl := s.dataURLFromID(opt.ID, batchEXT)

	recording := Recording{
		ID:      opt.ID,
		DataURL: dataUrl.String(),
		Type:    BatchRecording,
		Date:    time.Now(),
		Status:  Running,
	}
	err = s.recordings.Create(recording)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusInternalServerError)
		return
	}

	go func(recording Recording) {
		ds, _ := parseDataSourceURL(dataUrl.String())
		err := s.doRecordBatch(ds, t, opt.Start, opt.Stop, opt.Cluster)
		s.updateRecordingResult(recording, ds, err)
	}(recording)

	w.WriteHeader(http.StatusCreated)
	w.Write(httpd.MarshalJSON(convertRecording(recording), true))
}

func (s *Service) handleRecordQuery(w http.ResponseWriter, req *http.Request) {
	var opt kclient.RecordQueryOptions
	dec := json.NewDecoder(req.Body)
	err := dec.Decode(&opt)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusBadRequest)
		return
	}
	if opt.ID == "" {
		opt.ID = uuid.NewV4().String()
	}
	if !validID.MatchString(opt.ID) {
		httpd.HttpError(w, fmt.Sprintf("recording ID must contain only letters, numbers, '-', '.' and '_'. %q", opt.ID), true, http.StatusBadRequest)
		return
	}
	if opt.Query == "" {
		httpd.HttpError(w, "must provide query", true, http.StatusBadRequest)
		return
	}
	var dataUrl url.URL
	var typ RecordingType
	switch opt.Type {
	case kclient.StreamTask:
		dataUrl = s.dataURLFromID(opt.ID, streamEXT)
		typ = StreamRecording
	case kclient.BatchTask:
		dataUrl = s.dataURLFromID(opt.ID, batchEXT)
		typ = BatchRecording
	}

	recording := Recording{
		ID:      opt.ID,
		DataURL: dataUrl.String(),
		Type:    typ,
		Date:    time.Now(),
		Status:  Running,
	}
	err = s.recordings.Create(recording)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusInternalServerError)
		return
	}

	go func(recording Recording) {
		ds, _ := parseDataSourceURL(dataUrl.String())
		err := s.doRecordQuery(ds, opt.Query, typ, opt.Cluster)
		s.updateRecordingResult(recording, ds, err)
	}(recording)

	w.WriteHeader(http.StatusCreated)
	w.Write(httpd.MarshalJSON(convertRecording(recording), true))
}

func (s *Service) updateRecordingResult(recording Recording, ds DataSource, err error) {
	recording.Status = Finished
	if err != nil {
		recording.Status = Failed
		recording.Error = err.Error()
	}
	recording.Date = time.Now()
	recording.Progress = 1.0
	recording.Size, err = ds.Size()
	if err != nil {
		s.logger.Println("E! failed to determine size of recording", recording.ID, err)
	}

	err = s.recordings.Replace(recording)
	if err != nil {
		s.logger.Println("E! failed to save recording info", recording.ID, err)
	}
}
func (r *Service) updateReplayResult(replay Replay, err error) {
	replay.Status = Finished
	if err != nil {
		replay.Status = Failed
		replay.Error = err.Error()
	}
	replay.Progress = 1.0
	replay.Date = time.Now()
	err = r.replays.Replace(replay)
	if err != nil {
		r.logger.Println("E! failed to save replay results:", err)
	}
}

func (s *Service) handleReplay(w http.ResponseWriter, req *http.Request) {
	id, err := s.replayIDFromPath(req.URL.Path)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusBadRequest)
		return
	}
	replay, err := s.replays.Get(id)
	if err != nil {
		httpd.HttpError(w, "could not find replay: "+err.Error(), true, http.StatusNotFound)
		return
	}
	if replay.Status == Running {
		w.WriteHeader(http.StatusAccepted)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	w.Write(httpd.MarshalJSON(convertReplay(replay), true))
}

func (s *Service) handleDeleteReplay(w http.ResponseWriter, req *http.Request) {
	id, err := s.replayIDFromPath(req.URL.Path)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusBadRequest)
		return
	}
	//TODO: Cancel running replays
	s.replays.Delete(id)
	w.WriteHeader(http.StatusNoContent)
}

var allReplayFields = []string{
	"link",
	"id",
	"recording",
	"task",
	"recording-time",
	"clock",
	"date",
	"error",
	"status",
	"progress",
}

func (s *Service) handleListReplays(w http.ResponseWriter, r *http.Request) {
	pattern := r.URL.Query().Get("pattern")
	fields := r.URL.Query()["fields"]
	if len(fields) == 0 {
		fields = allReplayFields
	} else {
		// Always return ID field
		fields = append(fields, "id", "link")
	}

	var err error
	offset := int64(0)
	offsetStr := r.URL.Query().Get("offset")
	if offsetStr != "" {
		offset, err = strconv.ParseInt(offsetStr, 10, 64)
		if err != nil {
			httpd.HttpError(w, fmt.Sprintf("invalid offset parameter %q must be an integer: %s", offsetStr, err), true, http.StatusBadRequest)
		}
	}

	limit := int64(100)
	limitStr := r.URL.Query().Get("limit")
	if limitStr != "" {
		limit, err = strconv.ParseInt(limitStr, 10, 64)
		if err != nil {
			httpd.HttpError(w, fmt.Sprintf("invalid limit parameter %q must be an integer: %s", limitStr, err), true, http.StatusBadRequest)
		}
	}

	replays, err := s.replays.List(pattern, int(offset), int(limit))

	rs := make([]map[string]interface{}, len(replays))
	for i, replay := range replays {
		rs[i] = make(map[string]interface{}, len(fields))
		for _, field := range fields {
			var value interface{}
			switch field {
			case "id":
				value = replay.ID
			case "link":
				value = replayLink(replay.ID)
			case "recording":
				value = replay.RecordingID
			case "task":
				value = replay.TaskID
			case "recording-time":
				value = replay.RecordingTime
			case "clock":
				switch replay.Clock {
				case Fast:
					value = kclient.Fast
				case Real:
					value = kclient.Real
				}
			case "date":
				value = replay.Date
			case "error":
				value = replay.Error
			case "status":
				switch replay.Status {
				case Failed:
					value = kclient.Failed
				case Running:
					value = kclient.Running
				case Finished:
					value = kclient.Finished
				}
			case "progress":
				value = replay.Progress
			default:
				httpd.HttpError(w, fmt.Sprintf("unsupported field %q", field), true, http.StatusBadRequest)
				return
			}
			rs[i][field] = value
		}
	}
	type response struct {
		Replays []map[string]interface{} `json:"replays"`
	}
	w.Write(httpd.MarshalJSON(response{Replays: rs}, true))
}

func (s *Service) handleCreateReplay(w http.ResponseWriter, req *http.Request) {
	var opt kclient.CreateReplayOptions
	// Default clock to the Fast clock
	opt.Clock = kclient.Fast
	dec := json.NewDecoder(req.Body)
	err := dec.Decode(&opt)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusBadRequest)
		return
	}
	if opt.ID == "" {
		opt.ID = uuid.NewV4().String()
	}
	if !validID.MatchString(opt.ID) {
		httpd.HttpError(w, fmt.Sprintf("replay ID must contain only letters, numbers, '-', '.' and '_'. %q", opt.ID), true, http.StatusBadRequest)
		return
	}

	t, err := s.TaskStore.Load(opt.Task)
	if err != nil {
		httpd.HttpError(w, "task load: "+err.Error(), true, http.StatusNotFound)
		return
	}
	recording, err := s.recordings.Get(opt.Recording)
	if err != nil {
		httpd.HttpError(w, "recording not found: "+err.Error(), true, http.StatusNotFound)
		return
	}

	var clk clock.Clock
	var clockType Clock
	switch opt.Clock {
	case kclient.Real:
		clk = clock.Wall()
		clockType = Real
	case kclient.Fast:
		clk = clock.Fast()
		clockType = Fast
	default:
		httpd.HttpError(w, fmt.Sprintf("invalid clock type %v", opt.Clock), true, http.StatusBadRequest)
		return
	}

	// Successfully started replay
	replay := Replay{
		ID:            opt.ID,
		RecordingID:   opt.Recording,
		TaskID:        opt.Task,
		RecordingTime: opt.RecordingTime,
		Clock:         clockType,
		Date:          time.Now(),
		Status:        Running,
	}
	s.replays.Create(replay)

	go func(replay Replay) {
		err := s.doReplayFromRecording(t, recording, clk, opt.RecordingTime)
		s.updateReplayResult(replay, err)
	}(replay)

	w.WriteHeader(http.StatusCreated)
	w.Write(httpd.MarshalJSON(convertReplay(replay), true))
}

func (s *Service) handleReplayBatch(w http.ResponseWriter, req *http.Request) {
	var opt kclient.ReplayBatchOptions
	// Default clock to the Fast clock
	opt.Clock = kclient.Fast
	dec := json.NewDecoder(req.Body)
	err := dec.Decode(&opt)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusBadRequest)
		return
	}
	if opt.ID == "" {
		opt.ID = uuid.NewV4().String()
	}
	if !validID.MatchString(opt.ID) {
		httpd.HttpError(w, fmt.Sprintf("replay ID must match %v %q", validID, opt.ID), true, http.StatusBadRequest)
		return
	}

	t, err := s.TaskStore.Load(opt.Task)
	if err != nil {
		httpd.HttpError(w, "task load: "+err.Error(), true, http.StatusNotFound)
		return
	}

	var clk clock.Clock
	var clockType Clock
	switch opt.Clock {
	case kclient.Real:
		clk = clock.Wall()
		clockType = Real
	case kclient.Fast:
		clk = clock.Fast()
		clockType = Fast
	default:
		httpd.HttpError(w, fmt.Sprintf("invalid clock type %v", opt.Clock), true, http.StatusBadRequest)
		return
	}
	if t.Type == kapacitor.StreamTask {
		httpd.HttpError(w, fmt.Sprintf("cannot replay batch against stream task: %s", opt.Task), true, http.StatusBadRequest)
		return
	}

	// Successfully started replay
	replay := Replay{
		ID:            opt.ID,
		TaskID:        opt.Task,
		RecordingTime: opt.RecordingTime,
		Clock:         clockType,
		Date:          time.Now(),
		Status:        Running,
	}
	err = s.replays.Create(replay)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusInternalServerError)
		return
	}

	go func(replay Replay) {
		err := s.doLiveBatchReplay(t, clk, opt.RecordingTime, opt.Start, opt.Stop, opt.Cluster)
		s.updateReplayResult(replay, err)
	}(replay)

	w.WriteHeader(http.StatusCreated)
	w.Write(httpd.MarshalJSON(convertReplay(replay), true))
}

func (r *Service) handleReplayQuery(w http.ResponseWriter, req *http.Request) {
	var opt kclient.ReplayQueryOptions
	dec := json.NewDecoder(req.Body)
	err := dec.Decode(&opt)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusBadRequest)
		return
	}
	if opt.ID == "" {
		opt.ID = uuid.NewV4().String()
	}
	if !validID.MatchString(opt.ID) {
		httpd.HttpError(w, fmt.Sprintf("recording ID must match %v %q", validID, opt.ID), true, http.StatusBadRequest)
		return
	}
	if opt.Query == "" {
		httpd.HttpError(w, "must provide query", true, http.StatusBadRequest)
		return
	}

	t, err := r.TaskStore.Load(opt.Task)
	if err != nil {
		httpd.HttpError(w, "task load: "+err.Error(), true, http.StatusNotFound)
		return
	}

	var clk clock.Clock
	var clockType Clock
	switch opt.Clock {
	case kclient.Real:
		clk = clock.Wall()
		clockType = Real
	case kclient.Fast:
		clk = clock.Fast()
		clockType = Fast
	default:
		httpd.HttpError(w, fmt.Sprintf("invalid clock type %v", opt.Clock), true, http.StatusBadRequest)
		return
	}

	replay := Replay{
		ID:            opt.ID,
		TaskID:        opt.Task,
		RecordingTime: opt.RecordingTime,
		Clock:         clockType,
		Date:          time.Now(),
		Status:        Running,
	}
	err = r.replays.Create(replay)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusInternalServerError)
		return
	}

	go func(replay Replay) {
		err := r.doLiveQueryReplay(replay.ID, t, clk, opt.RecordingTime, opt.Query, opt.Cluster)
		r.updateReplayResult(replay, err)
	}(replay)

	w.WriteHeader(http.StatusCreated)
	w.Write(httpd.MarshalJSON(convertReplay(replay), true))
}

func (r *Service) doReplayFromRecording(task *kapacitor.Task, recording Recording, clk clock.Clock, recTime bool) error {
	dataSource, err := parseDataSourceURL(recording.DataURL)
	if err != nil {
		return errors.Wrap(err, "load data source")
	}
	runReplay := func(tm *kapacitor.TaskMaster) error {
		var replayC <-chan error
		switch task.Type {
		case kapacitor.StreamTask:
			f, err := dataSource.StreamReader()
			if err != nil {
				return errors.Wrap(err, "data source open")
			}
			stream, err := tm.Stream(recording.ID)
			if err != nil {
				return errors.Wrap(err, "stream start")
			}
			replayC = kapacitor.ReplayStreamFromIO(clk, f, stream, recTime, precision)
		case kapacitor.BatchTask:
			fs, err := dataSource.BatchReaders()
			if err != nil {
				return errors.Wrap(err, "data source open")
			}
			collectors := tm.BatchCollectors(task.ID)
			replayC = kapacitor.ReplayBatchFromIO(clk, fs, collectors, recTime)
		}
		return <-replayC
	}
	return r.doReplay(task, runReplay)

}

func (r *Service) doLiveBatchReplay(task *kapacitor.Task, clk clock.Clock, recTime bool, start, stop time.Time, cluster string) error {
	runReplay := func(tm *kapacitor.TaskMaster) error {
		sources, recordErrC, err := r.startRecordBatch(task, start, stop, cluster)
		if err != nil {
			return err
		}
		collectors := tm.BatchCollectors(task.ID)
		replayErrC := kapacitor.ReplayBatchFromChan(clk, sources, collectors, recTime)
		for i := 0; i < 2; i++ {
			var err error
			select {
			case err = <-replayErrC:
			case err = <-recordErrC:
			}
			if err != nil {
				return err
			}
		}
		return nil
	}
	return r.doReplay(task, runReplay)
}

func (r *Service) doLiveQueryReplay(id string, task *kapacitor.Task, clk clock.Clock, recTime bool, query, cluster string) error {
	runReplay := func(tm *kapacitor.TaskMaster) error {
		var replayErrC <-chan error
		runErrC := make(chan error, 1)
		switch task.Type {
		case kapacitor.StreamTask:
			source := make(chan models.Point)
			go func() {
				runErrC <- r.runQueryStream(source, query, cluster)
			}()
			stream, err := tm.Stream(id)
			if err != nil {
				return errors.Wrap(err, "stream start")
			}
			replayErrC = kapacitor.ReplayStreamFromChan(clk, source, stream, recTime)
		case kapacitor.BatchTask:
			source := make(chan models.Batch)
			go func() {
				runErrC <- r.runQueryBatch(source, query, cluster)
			}()
			collectors := tm.BatchCollectors(task.ID)
			replayErrC = kapacitor.ReplayBatchFromChan(clk, []<-chan models.Batch{source}, collectors, recTime)
		}
		for i := 0; i < 2; i++ {
			var err error
			select {
			case err = <-runErrC:
			case err = <-replayErrC:
			}
			if err != nil {
				return err
			}
		}
		return nil
	}
	return r.doReplay(task, runReplay)
}

func (r *Service) doReplay(task *kapacitor.Task, runReplay func(tm *kapacitor.TaskMaster) error) error {
	// Create new isolated task master
	tm := r.TaskMaster.New()
	tm.Open()
	defer tm.Close()
	et, err := tm.StartTask(task)
	if err != nil {
		return errors.Wrap(err, "task start")
	}
	err = runReplay(tm)
	if err != nil {
		return errors.Wrap(err, "running replay")
	}

	// Drain tm so the task can finish
	tm.Drain()

	// Stop stats nodes
	et.StopStats()

	// Check for error on task
	err = et.Wait()
	if err != nil {
		return errors.Wrap(err, "task run")
	}

	// Call close explicitly to check for error
	err = tm.Close()
	if err != nil {
		return errors.Wrap(err, "task master close")
	}
	return nil
}

// wrap gzipped writer and underlying file
type streamWriter struct {
	f  io.Closer
	gz io.WriteCloser
}

// write to gzip writer
func (s streamWriter) Write(b []byte) (int, error) {
	return s.gz.Write(b)
}

// close both gzip stream and file
func (s streamWriter) Close() error {
	s.gz.Close()
	return s.f.Close()
}

// Record the stream for a duration
func (s *Service) doRecordStream(id string, dataSource DataSource, stop time.Time, dbrps []kapacitor.DBRP, measurements []string) error {
	e, err := s.TaskMaster.NewFork(id, dbrps, measurements)
	if err != nil {
		return err
	}
	sw, err := dataSource.StreamWriter()
	if err != nil {
		return err
	}
	defer sw.Close()

	done := make(chan struct{})
	go func() {
		closed := false
		for p, ok := e.NextPoint(); ok; p, ok = e.NextPoint() {
			if closed {
				continue
			}
			if p.Time.After(stop) {
				closed = true
				close(done)
				//continue to read any data already on the edge, but just drop it.
				continue
			}
			kapacitor.WritePointForRecording(sw, p, precision)
		}
	}()
	<-done
	e.Abort()
	s.TaskMaster.DelFork(id)
	return nil
}

// wrap the underlying file and archive
type batchArchive struct {
	f       io.Closer
	archive *zip.Writer
}

// create new file in archive from batch index
func (b batchArchive) Archive(idx int) (io.Writer, error) {
	return b.archive.Create(strconv.FormatInt(int64(idx), 10))
}

// close both archive and file
func (b batchArchive) Close() error {
	err := b.archive.Close()
	if err != nil {
		b.f.Close()
		return err
	}
	return b.f.Close()
}

// Record a series of batch queries defined by a batch task
func (s *Service) doRecordBatch(dataSource DataSource, t *kapacitor.Task, start, stop time.Time, cluster string) error {
	sources, recordErrC, err := s.startRecordBatch(t, start, stop, cluster)
	if err != nil {
		return err
	}
	saveErrC := make(chan error, 1)
	go func() {
		saveErrC <- s.saveBatchRecording(dataSource, sources)
	}()
	for i := 0; i < 2; i++ {
		var err error
		select {
		case err = <-saveErrC:
		case err = <-recordErrC:
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) startRecordBatch(t *kapacitor.Task, start, stop time.Time, cluster string) ([]<-chan models.Batch, <-chan error, error) {
	et, err := kapacitor.NewExecutingTask(s.TaskMaster.New(), t)
	if err != nil {
		return nil, nil, err
	}

	batches, err := et.BatchQueries(start, stop)
	if err != nil {
		return nil, nil, err
	}

	if s.InfluxDBService == nil {
		return nil, nil, errors.New("InfluxDB not configured, cannot record batch query")
	}

	var con client.Client
	if cluster != "" {
		con, err = s.InfluxDBService.NewNamedClient(cluster)
	} else {
		con, err = s.InfluxDBService.NewDefaultClient()
	}
	if err != nil {
		return nil, nil, err
	}

	sources := make([]<-chan models.Batch, len(batches))
	errors := make(chan error, len(batches))

	for batchIdx, queries := range batches {
		source := make(chan models.Batch)
		sources[batchIdx] = source
		go func(queries []string) {
			defer close(source)
			for _, q := range queries {
				query := client.Query{
					Command: q,
				}
				resp, err := con.Query(query)
				if err != nil {
					errors <- err
					return
				}
				if err := resp.Error(); err != nil {
					errors <- err
					return
				}
				for _, res := range resp.Results {
					batches, err := models.ResultToBatches(res)
					if err != nil {
						errors <- err
						return
					}
					for _, b := range batches {
						source <- b
					}
				}
			}
			errors <- nil
		}(queries)
	}
	errC := make(chan error, 1)
	go func() {
		for i := 0; i < cap(errors); i++ {
			err := <-errors
			if err != nil {
				errC <- err
				return
			}
		}
		errC <- nil
	}()
	return sources, errC, nil
}

func (r *Service) saveBatchRecording(dataSource DataSource, sources []<-chan models.Batch) error {
	archiver, err := dataSource.BatchArchiver()
	if err != nil {
		return err
	}

	for batchIdx, batches := range sources {
		w, err := archiver.Archive(batchIdx)
		if err != nil {
			return err
		}
		for b := range batches {
			kapacitor.WriteBatchForRecording(w, b)
		}
	}
	return archiver.Close()
}

func (r *Service) doRecordQuery(dataSource DataSource, q string, typ RecordingType, cluster string) error {
	errC := make(chan error, 2)
	switch typ {
	case StreamRecording:
		points := make(chan models.Point)
		go func() {
			errC <- r.runQueryStream(points, q, cluster)
		}()
		go func() {
			errC <- r.saveStreamQuery(dataSource, points, precision)
		}()
	case BatchRecording:
		batches := make(chan models.Batch)
		go func() {
			errC <- r.runQueryBatch(batches, q, cluster)
		}()
		go func() {
			errC <- r.saveBatchQuery(dataSource, batches)
		}()
	}
	for i := 0; i < cap(errC); i++ {
		err := <-errC
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *Service) runQueryStream(source chan<- models.Point, q, cluster string) error {
	defer close(source)
	dbrp, resp, err := r.execQuery(q, cluster)
	if err != nil {
		return err
	}
	// Write results to sources
	for _, res := range resp.Results {
		batches, err := models.ResultToBatches(res)
		if err != nil {
			return err
		}
		// Write points in order across batches

		// Find earliest time of first points
		current := time.Time{}
		for _, batch := range batches {
			if len(batch.Points) > 0 &&
				(current.IsZero() ||
					batch.Points[0].Time.Before(current)) {
				current = batch.Points[0].Time
			}
		}

		finishedCount := 0
		batchCount := len(batches)
		for finishedCount != batchCount {
			next := time.Time{}
			for b := range batches {
				l := len(batches[b].Points)
				if l == 0 {
					finishedCount++
					continue
				}
				i := 0
				for ; i < l; i++ {
					bp := batches[b].Points[i]
					if bp.Time.After(current) {
						if next.IsZero() || bp.Time.Before(next) {
							next = bp.Time
						}
						break
					}
					// Write point
					p := models.Point{
						Name:            batches[b].Name,
						Database:        dbrp.Database,
						RetentionPolicy: dbrp.RetentionPolicy,
						Tags:            bp.Tags,
						Fields:          bp.Fields,
						Time:            bp.Time,
					}
					source <- p
				}
				// Remove written points
				batches[b].Points = batches[b].Points[i:]
			}
			current = next
		}
	}
	return nil
}

func (r *Service) runQueryBatch(source chan<- models.Batch, q string, cluster string) error {
	defer close(source)
	_, resp, err := r.execQuery(q, cluster)
	if err != nil {
		return err
	}
	// Write results to sources
	for _, res := range resp.Results {
		batches, err := models.ResultToBatches(res)
		if err != nil {
			return err
		}
		for _, batch := range batches {
			source <- batch
		}
	}
	return nil
}

func (r *Service) saveBatchQuery(dataSource DataSource, batches <-chan models.Batch) error {
	archiver, err := dataSource.BatchArchiver()
	if err != nil {
		return err
	}
	w, err := archiver.Archive(0)
	if err != nil {
		return err
	}

	for batch := range batches {
		err := kapacitor.WriteBatchForRecording(w, batch)
		if err != nil {
			return err
		}
	}

	return archiver.Close()
}

func (s *Service) saveStreamQuery(dataSource DataSource, points <-chan models.Point, precision string) error {
	sw, err := dataSource.StreamWriter()
	if err != nil {
		return err
	}
	for point := range points {
		err := kapacitor.WritePointForRecording(sw, point, precision)
		if err != nil {
			return err
		}
	}

	return sw.Close()
}

func (s *Service) execQuery(q, cluster string) (kapacitor.DBRP, *client.Response, error) {
	// Parse query to determine dbrp
	dbrp := kapacitor.DBRP{}
	stmt, err := influxql.ParseStatement(q)
	if err != nil {
		return dbrp, nil, err
	}
	if slct, ok := stmt.(*influxql.SelectStatement); ok && len(slct.Sources) == 1 {
		if m, ok := slct.Sources[0].(*influxql.Measurement); ok {
			dbrp.Database = m.Database
			dbrp.RetentionPolicy = m.RetentionPolicy
		}
	}
	if dbrp.Database == "" || dbrp.RetentionPolicy == "" {
		return dbrp, nil, errors.New("could not determine database and retention policy. Is the query fully qualified?")
	}
	if s.InfluxDBService == nil {
		return dbrp, nil, errors.New("InfluxDB not configured, cannot record query")
	}
	// Query InfluxDB
	var con client.Client
	if cluster != "" {
		con, err = s.InfluxDBService.NewNamedClient(cluster)
	} else {
		con, err = s.InfluxDBService.NewDefaultClient()
	}
	if err != nil {
		return dbrp, nil, err
	}
	query := client.Query{
		Command: q,
	}
	resp, err := con.Query(query)
	if err != nil {
		return dbrp, nil, err
	}
	if err := resp.Error(); err != nil {
		return dbrp, nil, err
	}
	return dbrp, resp, nil
}

type BatchArchiver interface {
	io.Closer
	Archive(idx int) (io.Writer, error)
}

type DataSource interface {
	Size() (int64, error)
	Remove() error
	StreamWriter() (io.WriteCloser, error)
	StreamReader() (io.ReadCloser, error)
	BatchArchiver() (BatchArchiver, error)
	BatchReaders() ([]io.ReadCloser, error)
}

type fileSource string

func parseDataSourceURL(rawurl string) (DataSource, error) {
	u, err := url.Parse(rawurl)
	if err != nil {
		return nil, err
	}
	switch u.Scheme {
	case "file":
		return fileSource(u.Path), nil
	default:
		return nil, fmt.Errorf("unsupported data source scheme %s", u.Scheme)
	}
}

func (s fileSource) Size() (int64, error) {
	info, err := os.Stat(string(s))
	if err != nil {
		return -1, err
	}
	return info.Size(), nil
}

func (s fileSource) Remove() error {
	return os.Remove(string(s))
}

func (s fileSource) StreamWriter() (io.WriteCloser, error) {
	f, err := os.Create(string(s))
	if err != nil {
		return nil, fmt.Errorf("failed to create recording file: %s", err)
	}
	gz := gzip.NewWriter(f)
	sw := streamWriter{f: f, gz: gz}
	return sw, nil
}

func (s fileSource) StreamReader() (io.ReadCloser, error) {
	f, err := os.Open(string(s))
	if err != nil {
		return nil, err
	}
	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	return rc{gz, f}, nil
}

func (s fileSource) BatchArchiver() (BatchArchiver, error) {
	f, err := os.Create(string(s))
	if err != nil {
		return nil, err
	}
	archive := zip.NewWriter(f)
	return &batchArchive{f: f, archive: archive}, nil
}
func (s fileSource) BatchReaders() ([]io.ReadCloser, error) {
	f, err := os.Open(string(s))
	if err != nil {
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	archive, err := zip.NewReader(f, stat.Size())
	if err != nil {
		return nil, err
	}
	rcs := make([]io.ReadCloser, len(archive.File))
	for i, file := range archive.File {
		rc, err := file.Open()
		if err != nil {
			return nil, err
		}
		rcs[i] = rc
	}
	return rcs, nil
}

type rc struct {
	r io.ReadCloser
	c io.Closer
}

func (r rc) Read(p []byte) (int, error) {
	return r.r.Read(p)
}

func (r rc) Close() error {
	err := r.r.Close()
	if err != nil {
		return err
	}
	err = r.c.Close()
	if err != nil {
		return err
	}
	return nil
}
