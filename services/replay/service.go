package replay

import (
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/influxdb/kapacitor"
	"github.com/influxdb/kapacitor/clock"
	"github.com/influxdb/kapacitor/services/httpd"
	"github.com/influxdb/kapacitor/wlog"
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
		Addr() string
	}
	TaskMaster interface {
		NewFork(name string) *kapacitor.Edge
		DelFork(name string)
		New() *kapacitor.TaskMaster
	}

	logger *log.Logger
}

// Create a new replay master.
func NewService(conf Config) *Service {
	return &Service{
		saveDir: conf.Dir,
		logger:  wlog.New(os.Stderr, "[replay] ", log.LstdFlags),
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

	t, err := r.TaskStore.Load(name)
	if err != nil {
		httpd.HttpError(w, "task load: "+err.Error(), true, http.StatusNotFound)
		return
	}

	f, err := r.Find(id, t.Type)
	if err != nil {
		httpd.HttpError(w, "replay find: "+err.Error(), true, http.StatusNotFound)
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
	switch t.Type {
	case kapacitor.StreamerTask:
		err = <-replay.ReplayStream(f, tm.Stream)
	case kapacitor.BatcherTask:
		batch := tm.BatchCollector(name)
		err = <-replay.ReplayBatch(f, batch)
	}

	if err != nil {
		httpd.HttpError(w, "replay: "+err.Error(), true, http.StatusInternalServerError)
		return
	}

	err = et.Err()
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
}

func (r *Service) handleRecord(w http.ResponseWriter, req *http.Request) {

	rid := uuid.NewV4()
	typ := req.URL.Query().Get("type")
	switch typ {
	case "stream":
		durStr := req.URL.Query().Get("duration")
		dur, err := time.ParseDuration(durStr)
		if err != nil {
			httpd.HttpError(w, "invalid duration string: "+err.Error(), true, http.StatusBadRequest)
			return
		}
		err = r.doRecordStream(rid, dur)
		if err != nil {
			httpd.HttpError(w, err.Error(), true, http.StatusInternalServerError)
			return
		}

	case "batch":
		num, err := strconv.ParseInt(req.URL.Query().Get("num"), 10, 64)
		if err != nil {
			httpd.HttpError(w, err.Error(), true, http.StatusBadRequest)
			return
		}

		var stop time.Time
		stopStr := req.URL.Query().Get("stop")
		if stopStr == "" {
			stop = time.Now()
		} else {
			stop, err = time.Parse(time.RFC3339, stopStr)
			if err != nil {
				httpd.HttpError(w, err.Error(), true, http.StatusBadRequest)
				return
			}
		}

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

		err = r.doRecordBatch(rid, t, r.InfluxDBService.Addr(), stop, int(num))
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
			typ = kapacitor.StreamerTask
		case batchEXT:
			typ = kapacitor.BatcherTask
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

func (r *Service) Find(id string, typ kapacitor.TaskType) (io.ReadCloser, error) {
	var ext string
	var other string
	switch typ {
	case kapacitor.StreamerTask:
		ext = streamEXT
		other = batchEXT
	case kapacitor.BatcherTask:
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
	gz, err := gzip.NewReader(f)
	return rc{gz, f}, nil
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
func (r *Service) doRecordStream(rid uuid.UUID, dur time.Duration) error {
	e := r.TaskMaster.NewFork(rid.String())
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
			fmt.Fprintf(gz, "%s\n%s\n", p.Database, p.RetentionPolicy)
			gz.Write(p.Bytes("s"))
			gz.Write([]byte("\n"))
		}
	}()
	time.Sleep(dur)
	done = true
	e.Close()
	r.TaskMaster.DelFork(rid.String())
	return nil
}

// Record a series of batch queries defined by a batcher task
func (r *Service) doRecordBatch(rid uuid.UUID, t *kapacitor.Task, addr string, stop time.Time, num int) error {
	query, err := t.Query()
	if err != nil {
		return err
	}
	period := t.Period()

	rpath := path.Join(r.saveDir, rid.String()+batchEXT)
	f, err := os.Create(rpath)
	if err != nil {
		return err
	}
	defer f.Close()
	gz := gzip.NewWriter(f)
	defer gz.Close()

	start := stop.Add(time.Duration(-num) * period)

	for i := 0; i < num; i++ {
		stop := start.Add(period)
		query.Start(start)
		query.Stop(stop)
		v := url.Values{}
		v.Add("q", query.String())
		res, err := http.Get(addr + "/query?" + v.Encode())
		if err != nil {
			return err
		}
		defer res.Body.Close()
		io.Copy(gz, res.Body)

		start = stop
	}
	return nil
}
