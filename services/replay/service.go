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
	"sync"
	"time"

	client "github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/kapacitor"
	"github.com/influxdata/kapacitor/clock"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/services/httpd"
	"github.com/twinj/uuid"
)

const streamEXT = ".srpl"
const batchEXT = ".brpl"

const precision = "n"

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
		NewDefaultClient() (client.Client, error)
		NewNamedClient(name string) (client.Client, error)
	}
	TaskMaster interface {
		NewFork(name string, dbrps []kapacitor.DBRP, measurements []string) (*kapacitor.Edge, error)
		DelFork(name string)
		New() *kapacitor.TaskMaster
		Stream(name string) (kapacitor.StreamCollector, error)
	}
	TagStore interface {
		ValidateTagSyntax(tag string) error
		ResolveTag(tagOrId string) (string, error)
		CreateOrUpdateTag(tag string, id string) error
		DeleteRecording(id string) error
	}

	recordingsMu      sync.RWMutex
	runningRecordings map[string]<-chan error

	logger *log.Logger
}

// Create a new replay master.
func NewService(conf Config, l *log.Logger) *Service {
	return &Service{
		saveDir:           conf.Dir,
		logger:            l,
		runningRecordings: make(map[string]<-chan error),
	}
}

func (r *Service) Open() error {
	r.routes = []httpd.Route{
		{
			Name:        "recordings",
			Method:      "GET",
			Pattern:     "/recordings",
			HandlerFunc: r.handleList,
		},
		{
			Name:        "recording-delete",
			Method:      "DELETE",
			Pattern:     "/recording",
			HandlerFunc: r.handleDelete,
		},
		{
			Name:        "recording-delete",
			Method:      "OPTIONS",
			Pattern:     "/recording",
			HandlerFunc: httpd.ServeOptions,
		},
		{
			Name:        "record",
			Method:      "POST",
			Pattern:     "/record",
			HandlerFunc: r.handleRecord,
		},
		{
			Name:        "record",
			Method:      "GET",
			Pattern:     "/record",
			HandlerFunc: r.handleGetRecording,
		},
		{
			Name:        "replay",
			Method:      "POST",
			Pattern:     "/replay",
			HandlerFunc: r.handleReplay,
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
		s.logger.Printf("E! list recordings for '%s' failed: %v", ridsStr, err)
		httpd.HttpError(w, err.Error(), true, http.StatusNotFound)
		return
	}

	type response struct {
		Recordings []RecordingInfo `json:"Recordings"`
	}

	w.Write(httpd.MarshalJSON(response{infos}, true))
}

func (s *Service) handleDelete(w http.ResponseWriter, r *http.Request) {
	rid := r.URL.Query().Get("rid")
	if err := s.Delete(rid); err != nil {
		s.logger.Printf("E! delete recordings for '%s' failed: %v", rid, err)
		httpd.HttpError(w, err.Error(), true, http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
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

	if id, err = r.TagStore.ResolveTag(id); err != nil {
		httpd.HttpError(w, "replay resolve: "+err.Error(), true, http.StatusNotFound)
		return
	}

	replay := kapacitor.NewReplay(clk)
	var replayC <-chan error
	switch t.Type {
	case kapacitor.StreamTask:
		f, err := r.FindStreamRecording(id, false)
		if err != nil {
			httpd.HttpError(w, "replay find: "+err.Error(), true, http.StatusNotFound)
			return
		}
		stream, err := tm.Stream(id)
		if err != nil {
			httpd.HttpError(w, "stream start: "+err.Error(), true, http.StatusInternalServerError)
			return
		}
		replayC = replay.ReplayStream(f, stream, recTime, precision)
	case kapacitor.BatchTask:
		fs, err := r.FindBatchRecording(id, false)
		if err != nil {
			httpd.HttpError(w, "replay find: "+err.Error(), true, http.StatusNotFound)
			return
		}
		batches := tm.BatchCollectors(name)
		replayC = replay.ReplayBatch(fs, batches, recTime)
	}

	// Check for error on replay
	err = <-replayC
	if err != nil {
		httpd.HttpError(w, "replay: "+err.Error(), true, http.StatusInternalServerError)
		return
	}

	// Drain tm so the task can finish
	tm.Drain()

	// Stop stats nodes
	et.StopStats()

	// Check for error on task
	err = et.Wait()
	if err != nil {
		httpd.HttpError(w, "task run: "+err.Error(), true, http.StatusInternalServerError)
		return
	}

	// Call close explicity to check for error
	err = tm.Close()
	if err != nil {
		httpd.HttpError(w, "closing: "+err.Error(), true, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (r *Service) handleRecord(w http.ResponseWriter, req *http.Request) {
	type doFunc func() error
	var doF doFunc
	started := make(chan struct{})

	rid := uuid.NewV4()
	typ := req.URL.Query().Get("type")
	tag := req.URL.Query().Get("tag")
	if err := r.checkTag(tag); err != nil {
		r.logger.Printf("E! record check tag '%s' failed: %v", tag, err)
		httpd.HttpError(w, err.Error(), true, http.StatusBadRequest)
		return
	}
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

		doF = func() error {
			err := r.doRecordStream(rid, dur, t.DBRPs, t.Measurements(), started, tag)
			if err != nil {
				close(started)
			}
			return err
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

		now := time.Now()

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
			start = now.Add(-1 * diff)
		}

		// Get stop time, if present
		stop := now
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

		// Get InfluxDB cluster
		cluster := req.URL.Query().Get("cluster")

		t, err := r.TaskStore.Load(task)
		if err != nil {
			httpd.HttpError(w, err.Error(), true, http.StatusNotFound)
			return
		}

		doF = func() error {
			close(started)
			return r.doRecordBatch(rid, t, start, stop, cluster, tag)
		}
	case "query":
		query := req.URL.Query().Get("query")
		if query == "" {
			httpd.HttpError(w, "must pass query parameter", true, http.StatusBadRequest)
			return
		}

		typeStr := req.URL.Query().Get("ttype")
		var tt kapacitor.TaskType
		switch typeStr {
		case "stream":
			tt = kapacitor.StreamTask
		case "batch":
			tt = kapacitor.BatchTask
		default:
			httpd.HttpError(w, fmt.Sprintf("invalid type %q", typeStr), true, http.StatusBadRequest)
			return
		}
		// Get InfluxDB cluster
		cluster := req.URL.Query().Get("cluster")
		doF = func() error {
			close(started)
			return r.doRecordQuery(rid, query, tt, cluster, tag)
		}
	default:
		httpd.HttpError(w, "invalid recording type", true, http.StatusBadRequest)
		return
	}
	// Store recording in running recordings.
	errC := make(chan error)
	func() {
		r.recordingsMu.Lock()
		defer r.recordingsMu.Unlock()
		r.runningRecordings[rid.String()] = errC
	}()

	// Spawn routine to perform actual recording.
	go func() {
		err := doF()
		if err != nil {
			// Always log an error since the user may not have requested the error.
			r.logger.Printf("E! recording %s failed: %v", rid.String(), err)
		}
		select {
		case errC <- err:
		case <-time.After(time.Minute):
			// Cache the error for a max duration then drop it
		}
		// We have finished delete from running map
		r.recordingsMu.Lock()
		defer r.recordingsMu.Unlock()
		delete(r.runningRecordings, rid.String())
	}()

	// Wait till the goroutine for doing the recording has actually started
	<-started

	// Respond with the recording ID
	type response struct {
		RecordingID string `json:"RecordingID"`
	}
	w.Write(httpd.MarshalJSON(response{rid.String()}, true))
}

func (r *Service) handleGetRecording(w http.ResponseWriter, req *http.Request) {
	tagOrId := req.URL.Query().Get("id")
	var rid string
	var err error
	if rid, err = r.TagStore.ResolveTag(tagOrId); err != nil {
		r.logger.Printf("E! get recording for '%s' failed: %v", tagOrId, err)
		httpd.HttpError(w, "error resolving recording ID or tag: "+err.Error(), true, http.StatusBadRequest)
		return
	}

	// First check if its still running
	var errC <-chan error
	var running bool
	func() {
		r.recordingsMu.RLock()
		defer r.recordingsMu.RUnlock()
		errC, running = r.runningRecordings[rid]
	}()

	if running {
		// It is still running wait for it to finish
		err := <-errC
		if err != nil {
			info := RecordingInfo{
				ID:    rid,
				Error: err.Error(),
			}
			w.Write(httpd.MarshalJSON(info, true))
			return
		}
	}

	// It already finished, return its info
	info, err := r.GetRecordings([]string{rid})
	if err != nil {
		r.logger.Printf("E! get recording '%s' failed: %v", rid, err)
		httpd.HttpError(w, "error finding recording: "+err.Error(), true, http.StatusInternalServerError)
		return
	}
	if len(info) != 1 {
		httpd.HttpError(w, "recording not found", true, http.StatusNotFound)
		return
	}

	w.Write(httpd.MarshalJSON(info[0], true))
}

type RecordingInfo struct {
	ID      string
	Type    kapacitor.TaskType
	Size    int64
	Created time.Time
	Error   string `json:",omitempty"`
}

func (r *Service) GetRecordings(rids []string) ([]RecordingInfo, error) {
	files, err := ioutil.ReadDir(r.saveDir)
	if err != nil {
		return nil, err
	}

	ids := make(map[string]bool)
	for _, rid := range rids {
		if id, err := r.TagStore.ResolveTag(rid); err != nil {
			ids[id] = true
		}
	}

	infos := make([]RecordingInfo, 0, len(files))

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
		rinfo := RecordingInfo{
			ID:      id,
			Type:    typ,
			Size:    info.Size(),
			Created: info.ModTime().UTC(),
		}
		infos = append(infos, rinfo)
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

func (r *Service) FindStreamRecording(id string, resolve bool) (io.ReadCloser, error) {
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

func (r *Service) FindBatchRecording(id string, resolve bool) ([]io.ReadCloser, error) {
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

func (r *Service) Delete(tagOrId string) error {
	if id, err := r.TagStore.ResolveTag(tagOrId); err != nil {
		return err
	} else {
		ps := path.Join(r.saveDir, id+streamEXT)
		pb := path.Join(r.saveDir, id+batchEXT)
		os.Remove(ps)
		os.Remove(pb)
		r.TagStore.DeleteRecording(id)
	}
	return nil
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

// create new stream writer
func (r *Service) newStreamWriter(rid uuid.UUID) (io.WriteCloser, error) {
	rpath := path.Join(r.saveDir, rid.String()+streamEXT)
	f, err := os.Create(rpath)
	if err != nil {
		return nil, fmt.Errorf("failed to save recording: %s", err)
	}
	gz := gzip.NewWriter(f)
	sw := streamWriter{f: f, gz: gz}
	return sw, nil
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
func (r *Service) doRecordStream(rid uuid.UUID, dur time.Duration, dbrps []kapacitor.DBRP, measurements []string, started chan struct{}, tag string) (err error) {
	defer func() {
		if err == nil {
			err = r.createTag(rid, tag)
		}
	}()

	e, err := r.TaskMaster.NewFork(rid.String(), dbrps, measurements)
	if err != nil {
		return err
	}
	sw, err := r.newStreamWriter(rid)
	if err != nil {
		return err
	}
	defer sw.Close()

	done := make(chan struct{})
	go func() {
		close(started)
		start := time.Time{}
		closed := false
		for p, ok := e.NextPoint(); ok; p, ok = e.NextPoint() {
			if closed {
				continue
			}
			if start.IsZero() {
				start = p.Time
			}
			if p.Time.Sub(start) > dur {
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
	r.TaskMaster.DelFork(rid.String())
	return nil
}

// open an archive for writing batch recordings
func (r *Service) newBatchArchive(rid uuid.UUID) (*batchArchive, error) {
	rpath := path.Join(r.saveDir, rid.String()+batchEXT)
	f, err := os.Create(rpath)
	if err != nil {
		return nil, err
	}
	archive := zip.NewWriter(f)
	return &batchArchive{f: f, archive: archive}, nil
}

// wrap the underlying file and archive
type batchArchive struct {
	f       io.Closer
	archive *zip.Writer
}

// create new file in archive from batch index
func (b batchArchive) Create(idx int) (io.Writer, error) {
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

// check that if a tag is specified, it has the correct syntax
func (r *Service) checkTag(tag string) error {
	if tag == "" {
		return nil
	} else {
		return r.TagStore.ValidateTagSyntax(tag)
	}
}

// if a tag was specified, then create or update a tag for the specified recording
func (r *Service) createTag(rid uuid.UUID, tag string) error {
	if tag != "" {
		return r.TagStore.CreateOrUpdateTag(tag, rid.String())
	}
	return nil
}

// Record a series of batch queries defined by a batch task
func (r *Service) doRecordBatch(rid uuid.UUID, t *kapacitor.Task, start, stop time.Time, cluster string, tag string) (err error) {

	defer func() {
		if err == nil {
			err = r.createTag(rid, tag)
		}
	}()

	et, err := kapacitor.NewExecutingTask(r.TaskMaster.New(), t)
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

	var con client.Client
	if cluster != "" {
		con, err = r.InfluxDBService.NewNamedClient(cluster)
	} else {
		con, err = r.InfluxDBService.NewDefaultClient()
	}
	if err != nil {
		return err
	}

	archive, err := r.newBatchArchive(rid)
	if err != nil {
		return err
	}

	for batchIdx, queries := range batches {
		w, err := archive.Create(batchIdx)
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
			if err := resp.Error(); err != nil {
				return err
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

func (r *Service) doRecordQuery(rid uuid.UUID, q string, tt kapacitor.TaskType, cluster string, tag string) (err error) {
	defer func() {
		if err == nil {
			err = r.createTag(rid, tag)
		}
	}()

	// Parse query to determine dbrp
	var db, rp string
	s, err := influxql.ParseStatement(q)
	if err != nil {
		return err
	}
	if slct, ok := s.(*influxql.SelectStatement); ok && len(slct.Sources) == 1 {
		if m, ok := slct.Sources[0].(*influxql.Measurement); ok {
			db = m.Database
			rp = m.RetentionPolicy
		}
	}
	if db == "" || rp == "" {
		return errors.New("could not determine database and retention policy. Is the query fully qualified?")
	}
	if r.InfluxDBService == nil {
		return errors.New("InfluxDB not configured, cannot record query")
	}
	// Query InfluxDB
	var con client.Client
	if cluster != "" {
		con, err = r.InfluxDBService.NewNamedClient(cluster)
	} else {
		con, err = r.InfluxDBService.NewDefaultClient()
	}
	if err != nil {
		return err
	}
	query := client.Query{
		Command: q,
	}
	resp, err := con.Query(query)
	if err != nil {
		return err
	}
	if err := resp.Error(); err != nil {
		return err
	}
	// Open appropriate writer
	var w io.Writer
	var c io.Closer
	switch tt {
	case kapacitor.StreamTask:
		sw, err := r.newStreamWriter(rid)
		if err != nil {
			return err
		}
		w = sw
		c = sw
	case kapacitor.BatchTask:
		archive, err := r.newBatchArchive(rid)
		if err != nil {
			return err
		}
		w, err = archive.Create(0)
		if err != nil {
			return err
		}
		c = archive
	}
	// Write results to writer
	for _, res := range resp.Results {
		batches, err := models.ResultToBatches(res)
		if err != nil {
			c.Close()
			return err
		}
		for _, batch := range batches {
			switch tt {
			case kapacitor.StreamTask:
				for _, bp := range batch.Points {
					p := models.Point{
						Name:            batch.Name,
						Database:        db,
						RetentionPolicy: rp,
						Tags:            bp.Tags,
						Fields:          bp.Fields,
						Time:            bp.Time,
					}
					kapacitor.WritePointForRecording(w, p, precision)
				}
			case kapacitor.BatchTask:
				kapacitor.WriteBatchForRecording(w, batch)
			}
		}
	}
	return c.Close()
}
