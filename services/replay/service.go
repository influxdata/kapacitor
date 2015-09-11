package replay

import (
	"compress/gzip"
	"io"
	"net"
	"net/http"
	"os"
	"path"
	"time"

	"github.com/influxdb/kapacitor"
	"github.com/influxdb/kapacitor/clock"
	"github.com/influxdb/kapacitor/services/httpd"
	"github.com/twinj/uuid"
)

const ext = ".rpl"

// Handles recording, starting, and waiting on replays
type Service struct {
	saveDir   string
	TaskStore interface {
		Load(name string) (*kapacitor.Task, error)
	}
	HTTPDService interface {
		AddRoutes([]httpd.Route) error
		DelRoutes([]httpd.Route)
		Addr() net.Addr
	}
	TaskMaster interface {
		NewFork(name string) *kapacitor.Edge
		DelFork(name string)
	}
}

// Create a new replay master.
func NewService(conf Config) *Service {
	return &Service{
		saveDir: conf.Dir,
	}
}

func (r *Service) Open() error {
	routes := []httpd.Route{
		{
			"replay",
			"POST",
			"/replay",
			true,
			true,
			r.handleReplay,
		},
		{
			"record",
			"POST",
			"/record",
			true,
			true,
			r.handleRecord,
		},
	}
	return r.HTTPDService.AddRoutes(routes)
}

func (r *Service) Close() error {
	return nil
}

func (r *Service) handleReplay(w http.ResponseWriter, req *http.Request) {
	name := req.URL.Query().Get("name")
	id := req.URL.Query().Get("id")
	clockTyp := req.URL.Query().Get("clock")
	f, err := r.Find(id)
	if err != nil {
		httpd.HttpError(w, "replay find: "+err.Error(), true, http.StatusNotFound)
		return
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
	tm := kapacitor.NewTaskMaster()
	tm.HTTPDService = r.HTTPDService
	tm.Open()
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
		e := r.TaskMaster.NewFork(rid.String())
		rpath := path.Join(r.saveDir, rid.String()+ext)
		f, err := os.Create(rpath)
		if err != nil {
			httpd.HttpError(w, "failed to save recording: "+err.Error(), true, http.StatusInternalServerError)
			return
		}
		defer f.Close()
		gz := gzip.NewWriter(f)
		defer gz.Close()

		done := false
		go func() {
			for p := e.NextPoint(); p != nil && !done; p = e.NextPoint() {
				gz.Write(p.Bytes("s"))
				gz.Write([]byte("\n"))
			}
		}()
		time.Sleep(dur)
		done = true
		e.Close()
		r.TaskMaster.DelFork(rid.String())
		type response struct {
			RecordingID string `json:"RecordingID"`
		}
		w.Write(httpd.MarshalJSON(response{rid.String()}, true))
	case "query":
	default:
		httpd.HttpError(w, "invalid recording type", true, http.StatusBadRequest)
		return
	}
}

func (r *Service) Find(id string) (io.ReadCloser, error) {
	p := path.Join(r.saveDir, id+ext)
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	gz, err := gzip.NewReader(f)
	return rc{gz, f}, nil
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
