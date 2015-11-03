package replay

import (
	"archive/zip"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/influxdb/influxdb/client"
	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/kapacitor"
	"github.com/influxdb/kapacitor/clock"
	"github.com/influxdb/kapacitor/models"
	"github.com/influxdb/kapacitor/services/httpd"
	"github.com/twinj/uuid"
)

const streamEXT = ".srpl"
const batchEXT = ".brpl"

// Handles recording, starting, and waiting on replays
type Service struct {
	saveDir   string
	routes    []httpd.Route
	TaskStore interface {
		Load(name string) (*kapacitor.Task, error)
	}
	HTTPDService interface {
		AddRoutes([]httpd.Route) error
		DelRoutes([]httpd.Route)
	}
	InfluxDBService interface {
		NewClient() (*client.Client, error)
	}
	TaskMaster interface {
		NewFork(name string, dbrps []kapacitor.DBRP) *kapacitor.Edge
		DelFork(name string)
		New() *kapacitor.TaskMaster
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

func (r *Service) Open() error {
	r.routes = []httpd.Route{
		{
			"recordings",
			"GET",
			"/recordings",
			true,
			true,
			r.handleList,
		},
		{
			"recording-delete",
			"DELETE",
			"/recording",
			true,
			true,
			r.handleDelete,
		},
		{
			"record",
			"POST",
			"/record",
			true,
			true,
			r.handleRecord,
		},
		{
			"replay",
			"POST",
			"/replay",
			true,
			true,
			r.handleReplay,
		},
	}

	err := os.MkdirAll(r.saveDir, 0755)
	if err != nil {
		return err
	}

	return r.HTTPDService.AddRoutes(r.routes)
}

func (r *Service) Close() error {
	r.HTTPDService.DelRoutes(r.routes)
	return nil
}

func (s *Service) handleList(w http.ResponseWriter, req *http.Request) {
	ridsStr := req.URL.Query().Get("rids")
	var rids []string
	if ridsStr != "" {
		rids = strings.Split(ridsStr, ",")
	}

	infos, err := s.GetRecordings(rids)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusNotFound)
		return
	}

	type response struct {
		Recordings []recordingInfo `json:"Recordings"`
	}

	w.Write(httpd.MarshalJSON(response{infos}, true))
}

func (s *Service) handleDelete(w http.ResponseWriter, r *http.Request) {
	rid := r.URL.Query().Get("rid")
	s.Delete(rid)
}

func (r *Service) handleReplay(w http.ResponseWriter, req *http.Request) {
	name := req.URL.Query().Get("name")
	id := req.URL.Query().Get("id")
	clockTyp := req.URL.Query().Get("clock")
	recTimeStr := req.URL.Query().Get("rec-time")
	var recTime bool
	if recTimeStr != "" {
		var err error
		recTime, err = strconv.ParseBool(recTimeStr)
		if err != nil {
			httpd.HttpError(w, err.Error(), true, http.StatusBadRequest)
			return
		}
	}

	t, err := r.TaskStore.Load(name)
	if err != nil {
		httpd.HttpError(w, "task load: "+err.Error(), true, http.StatusNotFound)
		return
	}

	var clk clock.Clock
	switch clockTyp {
	case "", "wall":
		clk = clock.Wall()
	case "fast":
		clk = clock.Fast()
	}

	// Create new isolated task master
	tm := r.TaskMaster.New()
	tm.Open()
	defer tm.Close()
	et, err := tm.StartTask(t)
	if err != nil {
		httpd.HttpError(w, "task start: "+err.Error(), true, http.StatusBadRequest)
		return
	}

	replay := kapacitor.NewReplay(clk)
	var replayC <-chan error
	switch t.Type {
	case kapacitor.StreamTask:
		f, err := r.FindStreamRecording(id)
		if err != nil {
			httpd.HttpError(w, "replay find: "+err.Error(), true, http.StatusNotFound)
			return
		}
		replayC = replay.ReplayStream(f, tm.Stream, recTime)
	case kapacitor.BatchTask:
		fs, err := r.FindBatchRecording(id)
		if err != nil {
			httpd.HttpError(w, "replay find: "+err.Error(), true, http.StatusNotFound)
			return
		}
		batches := tm.BatchCollectors(name)
		replayC = replay.ReplayBatch(fs, batches, recTime)
	}

	// Check first for error on task
	err = et.Err()
	if err != nil {
		httpd.HttpError(w, "task run: "+err.Error(), true, http.StatusInternalServerError)
		return
	}

	// Check for error on replay
	err = <-replayC
	if err != nil {
		httpd.HttpError(w, "replay: "+err.Error(), true, http.StatusInternalServerError)
		return
	}

	// Call close explicity to check for error
	err = tm.Close()
	if err != nil {
		httpd.HttpError(w, "closing: "+err.Error(), true, http.StatusInternalServerError)
		return
	}
}

func (r *Service) handleRecord(w http.ResponseWriter, req *http.Request) {

	rid := uuid.NewV4()
	typ := req.URL.Query().Get("type")
	switch typ {
	case "stream":
		task := req.URL.Query().Get("name")
		if task == "" {
			httpd.HttpError(w, "no task specified", true, http.StatusBadRequest)
			return
		}
		t, err := r.TaskStore.Load(task)
		if err != nil {
			httpd.HttpError(w, err.Error(), true, http.StatusNotFound)
			return
		}

		durStr := req.URL.Query().Get("duration")
		dur, err := influxql.ParseDuration(durStr)
		if err != nil {
			httpd.HttpError(w, "invalid duration string: "+err.Error(), true, http.StatusBadRequest)
			return
		}

		err = r.doRecordStream(rid, dur, t.DBRPs)
		if err != nil {
			httpd.HttpError(w, err.Error(), true, http.StatusInternalServerError)
			return
		}

	case "batch":
		var err error

		// Determine start time.
		var start time.Time
		startStr := req.URL.Query().Get("start")
		pastStr := req.URL.Query().Get("past")
		if startStr != "" && pastStr != "" {
			httpd.HttpError(w, "must not pass both 'start' and 'past' parameters", true, http.StatusBadRequest)
			return
		}

		switch {
		case startStr != "":
			start, err = time.Parse(time.RFC3339, startStr)
			if err != nil {
				httpd.HttpError(w, err.Error(), true, http.StatusBadRequest)
				return
			}
		case pastStr != "":
			diff, err := influxql.ParseDuration(pastStr)
			if err != nil {
				httpd.HttpError(w, err.Error(), true, http.StatusBadRequest)
				return
			}
			start = time.Now().Add(-1 * diff)
		}

		// Get stop time, if present
		var stop time.Time
		stopStr := req.URL.Query().Get("stop")
		if stopStr != "" {
			stop, err = time.Parse(time.RFC3339, stopStr)
			if err != nil {
				httpd.HttpError(w, err.Error(), true, http.StatusBadRequest)
				return
			}
		}

		// Get task
		task := req.URL.Query().Get("name")
		if task == "" {
			httpd.HttpError(w, "no task specified", true, http.StatusBadRequest)
			return
		}

		t, err := r.TaskStore.Load(task)
		if err != nil {
			httpd.HttpError(w, err.Error(), true, http.StatusNotFound)
			return
		}

		// Record batch data
		err = r.doRecordBatch(rid, t, start, stop)
		if err != nil {
			httpd.HttpError(w, err.Error(), true, http.StatusInternalServerError)
			return
		}
	case "query":
		httpd.HttpError(w, "not implemented", true, http.StatusInternalServerError)
		return

	default:
		httpd.HttpError(w, "invalid recording type", true, http.StatusBadRequest)
		return
	}
	// Respond with the recording ID
	type response struct {
		RecordingID string `json:"RecordingID"`
	}
	w.Write(httpd.MarshalJSON(response{rid.String()}, true))
}

type recordingInfo struct {
	ID   string
	Type kapacitor.TaskType
	Size int64
}

func (r *Service) GetRecordings(rids []string) ([]recordingInfo, error) {
	files, err := ioutil.ReadDir(r.saveDir)
	if err != nil {
		return nil, err
	}

	ids := make(map[string]bool)
	for _, id := range rids {
		ids[id] = true
	}

	infos := make([]recordingInfo, 0, len(files))

	for _, info := range files {
		if info.IsDir() {
			continue
		}
		name := info.Name()
		i := strings.LastIndex(name, ".")
		ext := name[i:]
		id := name[:i]
		if len(ids) > 0 && !ids[id] {
			continue
		}
		var typ kapacitor.TaskType
		switch ext {
		case streamEXT:
			typ = kapacitor.StreamTask
		case batchEXT:
			typ = kapacitor.BatchTask
		default:
			continue
		}
		info := recordingInfo{
			ID:   id,
			Type: typ,
			Size: info.Size(),
		}
		infos = append(infos, info)
	}
	return infos, nil
}

func (r *Service) find(id string, typ kapacitor.TaskType) (*os.File, error) {
	var ext string
	var other string
	switch typ {
	case kapacitor.StreamTask:
		ext = streamEXT
		other = batchEXT
	case kapacitor.BatchTask:
		ext = batchEXT
		other = streamEXT
	default:
		return nil, fmt.Errorf("unknown task type %q", typ)
	}
	p := path.Join(r.saveDir, id+ext)
	f, err := os.Open(p)
	if err != nil {
		if _, err := os.Stat(path.Join(r.saveDir, id+other)); err == nil {
			return nil, fmt.Errorf("found recording of wrong type, check task type matches recording.")
		}
		return nil, err
	}
	return f, nil
}

func (r *Service) FindStreamRecording(id string) (io.ReadCloser, error) {
	f, err := r.find(id, kapacitor.StreamTask)
	if err != nil {
		return nil, err
	}
	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	return rc{gz, f}, nil
}

func (r *Service) FindBatchRecording(id string) ([]io.ReadCloser, error) {
	f, err := r.find(id, kapacitor.BatchTask)
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

func (r *Service) Delete(id string) {
	ps := path.Join(r.saveDir, id+streamEXT)
	pb := path.Join(r.saveDir, id+batchEXT)
	os.Remove(ps)
	os.Remove(pb)
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

// Record the stream for a duration
func (r *Service) doRecordStream(rid uuid.UUID, dur time.Duration, dbrps []kapacitor.DBRP) error {
	e := r.TaskMaster.NewFork(rid.String(), dbrps)
	rpath := path.Join(r.saveDir, rid.String()+streamEXT)
	f, err := os.Create(rpath)
	if err != nil {
		return fmt.Errorf("failed to save recording: %s", err)
	}
	defer f.Close()
	gz := gzip.NewWriter(f)
	defer gz.Close()

	done := false
	go func() {
		for p, ok := e.NextPoint(); ok && !done; p, ok = e.NextPoint() {
			kapacitor.WritePointForRecording(gz, p)
		}
	}()
	time.Sleep(dur)
	done = true
	e.Close()
	r.TaskMaster.DelFork(rid.String())
	return nil
}

// Record a series of batch queries defined by a batch task
func (r *Service) doRecordBatch(rid uuid.UUID, t *kapacitor.Task, start, stop time.Time) error {
	et, err := kapacitor.NewExecutingTask(nil, t)
	if err != nil {
		return err
	}

	batches, err := et.BatchQueries(start, stop)
	if err != nil {
		return err
	}

	if r.InfluxDBService == nil {
		return errors.New("InfluxDB not configured, cannot record batch query")
	}
	con, err := r.InfluxDBService.NewClient()
	if err != nil {
		return err
	}
	rpath := path.Join(r.saveDir, rid.String()+batchEXT)
	f, err := os.Create(rpath)
	if err != nil {
		return err
	}
	defer f.Close()
	archive := zip.NewWriter(f)

	for batchIdx, queries := range batches {
		w, err := archive.Create(strconv.FormatInt(int64(batchIdx), 10))
		if err != nil {
			return err
		}
		for _, q := range queries {
			query := client.Query{
				Command: q,
			}
			resp, err := con.Query(query)
			if err != nil {
				return err
			}
			if resp.Err != nil {
				return resp.Err
			}
			for _, res := range resp.Results {
				batches, err := models.ResultToBatches(res)
				if err != nil {
					return err
				}
				for _, b := range batches {
					kapacitor.WriteBatchForRecording(w, b)
				}
			}
		}
	}
	return archive.Close()
}
