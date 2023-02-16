package alerttest

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/influxdb/pkg/errors"
	"github.com/influxdata/kapacitor/alert"
	"github.com/influxdata/kapacitor/command"
	"github.com/influxdata/kapacitor/command/commandtest"
	salert "github.com/influxdata/kapacitor/services/alert"
)

type Log struct {
	path string
}

func NewLog(p string) *Log {
	return &Log{
		path: p,
	}
}

func (l *Log) Data() (d []alert.Data, rErr error) {
	f, err := os.Open(l.path)
	if err != nil {
		return nil, err
	}
	defer errors.Capture(&rErr, f.Close)()
	dec := json.NewDecoder(f)
	var data []alert.Data
	for dec.More() {
		ad := alert.Data{}
		err := dec.Decode(&ad)
		if err != nil {
			return nil, err
		}
		data = append(data, ad)
	}
	return data, nil
}

func (l *Log) Mode() (os.FileMode, error) {
	stat, err := os.Stat(l.path)
	if err != nil {
		return 0, err
	}
	return stat.Mode(), nil
}

type Exec struct {
	ct        *commandtest.Commander
	Commander command.Commander
}

func NewExec() *Exec {
	ct := new(commandtest.Commander)
	return &Exec{
		ct:        ct,
		Commander: ct,
	}
}

func (e *Exec) Commands() []*commandtest.Command {
	return e.ct.Commands()
}

type TCPServer struct {
	Addr string

	l *net.TCPListener

	data []alert.Data

	wg     sync.WaitGroup
	closed bool
}

func NewTCPServer() (*TCPServer, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return nil, err
	}
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return nil, err
	}
	s := &TCPServer{
		l:    l,
		Addr: l.Addr().String(),
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.run()
	}()
	return s, nil
}

func (s *TCPServer) Data() []alert.Data {
	return s.data
}

func (s *TCPServer) Close() {
	if s.closed {
		return
	}
	s.closed = true
	s.l.Close()
	s.wg.Wait()
}

func (s *TCPServer) run() {
	for {
		conn, err := s.l.Accept()
		if err != nil {
			return
		}
		func() {
			defer conn.Close()
			ad := alert.Data{}
			json.NewDecoder(conn).Decode(&ad)
			s.data = append(s.data, ad)
		}()
	}
}

type PostServer struct {
	ts     *httptest.Server
	URL    string
	data   []alert.Data
	closed bool
}

func NewPostServer() *PostServer {
	s := new(PostServer)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ad := alert.Data{}
		dec := json.NewDecoder(r.Body)
		dec.Decode(&ad)
		s.data = append(s.data, ad)
	}))
	s.ts = ts
	s.URL = ts.URL
	return s
}

func (s *PostServer) Data() []alert.Data {
	return s.data
}

func (s *PostServer) Close() {
	if s.closed {
		return
	}
	s.closed = true
	s.ts.Close()
}

// helpers

type EventStateSpec struct {
	N   int
	Mwc int // message word count
	Dwc int // details word count
}

func makeSentence(seededRand *rand.Rand, n int) string {
	s := make([]string, n)
	for i := 0; i < n; i++ {
		s[i] = words[seededRand.Int31n(int32(len(words)))]
	}
	return strings.Join(s, " ")
}

func MakeEventStates(s EventStateSpec) map[string]salert.EventState {
	// Force rand sequence to be deterministic.
	seededRand := rand.New(rand.NewSource(int64(s.N)))

	es := make(map[string]salert.EventState, s.N)
	for i := 0; i < s.N; i++ {
		es[fmt.Sprintf("event_state_id_%d", i)] = salert.EventState{
			Message:  makeSentence(seededRand, s.Mwc),
			Details:  makeSentence(seededRand, s.Dwc),
			Time:     time.Unix(0, int64(i*1e9)),
			Duration: time.Duration((i * int(time.Millisecond)) % 10 * int(time.Second)),
			Level:    alert.Level(i % 0x3), // assumes levels 0-3
		}
	}
	return es
}

var words = [...]string{
	"lorem", "ipsum", "dolor", "sit", "amet", "consectetuer", "adipiscing", "elit", "integer", "in", "mi", "a", "mauris",
	"ornare", "sagittis", "suspendisse", "potenti", "suspendisse", "dapibus", "dignissim", "dolor", "nam",
	"sapien", "tellus", "tempus", "et", "tempus", "ac", "tincidunt", "in", "arcu", "duis", "dictum", "proin", "magna",
	"nulla", "pellentesque", "non", "commodo", "et", "iaculis", "sit", "amet", "mi", "mauris", "condimentum", "massa",
	"ut", "metus", "donec", "viverra", "sapien", "mattis", "rutrum", "tristique", "lacus", "eros", "semper", "tellus",
	"et", "molestie", "nisi", "sapien", "eu", "massa", "vestibulum", "ante", "ipsum", "primis", "in", "faucibus", "orci",
	"luctus", "et", "ultrices", "posuere", "cubilia", "curae", "fusce", "erat", "tortor", "mollis", "ut", "accumsan",
	"ut", "lacinia", "gravida", "libero", "curabitur", "massa", "felis", "accumsan", "feugiat", "convallis", "sit",
	"amet", "porta", "vel", "neque", "duis", "et", "ligula", "non", "elit", "ultricies", "rutrum", "suspendisse",
	"tempor", "quisque", "posuere", "malesuada", "velit", "sed", "pellentesque", "mi", "a", "purus", "integer",
	"imperdiet", "orci", "a", "eleifend", "mollis", "velit", "nulla", "iaculis", "arcu", "eu", "rutrum", "magna", "quam",
	"sed", "elit", "nullam", "egestas", "integer", "interdum", "purus", "nec", "mauris", "vestibulum", "ac", "mi", "in",
	"nunc", "suscipit", "dapibus", "duis", "consectetuer", "ipsum", "et", "pharetra", "sollicitudin", "metus",
	"turpis", "facilisis", "magna", "vitae", "dictum", "ligula", "nulla", "nec", "mi", "nunc", "ante", "urna", "gravida",
	"sit", "amet", "congue", "et", "accumsan", "vitae", "magna", "praesent", "luctus", "nullam", "in", "velit",
	"praesent", "est", "curabitur", "turpis", "class", "aptent", "taciti", "sociosqu", "ad", "litora", "torquent",
	"per", "conubia", "nostra", "per", "inceptos", "hymenaeos", "cras", "consectetuer", "nibh", "in", "lacinia",
	"ornare", "turpis", "sem", "tempor", "massa", "sagittis", "feugiat", "mauris", "nibh", "non", "tellus",
	"phasellus", "mi", "fusce", "enim", "mauris", "ultrices", "turpis", "eu", "adipiscing", "viverra", "justo",
	"libero", "ullamcorper", "massa", "id", "ultrices", "velit", "est", "quis", "tortor", "quisque", "condimentum",
	"lacus", "volutpat", "nonummy", "accumsan", "est", "nunc", "imperdiet", "magna", "vulputate", "aliquet", "nisi",
	"risus", "at", "est", "aliquam", "imperdiet", "gravida", "tortor", "praesent", "interdum", "accumsan", "ante",
	"vivamus", "est", "ligula", "consequat", "sed", "pulvinar", "eu", "consequat", "vitae", "eros", "nulla", "elit",
	"nunc", "congue", "eget", "scelerisque", "a", "tempor", "ac", "nisi", "morbi", "facilisis", "pellentesque",
	"habitant", "morbi", "tristique", "senectus", "et", "netus", "et", "malesuada", "fames", "ac", "turpis", "egestas",
	"in", "hac", "habitasse", "platea", "dictumst", "suspendisse", "vel", "lorem", "ut", "ligula", "tempor",
	"consequat", "quisque", "consectetuer", "nisl", "eget", "elit", "proin", "quis", "mauris", "ac", "orci",
	"accumsan", "suscipit", "sed", "ipsum", "sed", "vel", "libero", "nec", "elit", "feugiat", "blandit", "vestibulum",
	"purus", "nulla", "accumsan", "et", "volutpat", "at", "pellentesque", "vel", "urna", "suspendisse", "nonummy",
	"aliquam", "pulvinar", "libero", "donec", "vulputate", "orci", "ornare", "bibendum", "condimentum", "lorem",
	"elit", "dignissim", "sapien", "ut", "aliquam", "nibh", "augue", "in", "turpis", "phasellus", "ac", "eros",
	"praesent", "luctus", "lorem", "a", "mollis", "lacinia", "leo", "turpis", "commodo", "sem", "in", "lacinia", "mi",
	"quam", "et", "quam", "curabitur", "a", "libero", "vel", "tellus", "mattis", "imperdiet", "in", "congue", "neque", "ut",
	"scelerisque", "bibendum", "libero", "lacus", "ullamcorper", "sapien", "quis", "aliquet", "massa", "velit",
	"vel", "orci", "fusce", "in", "nulla", "quis", "est", "cursus", "gravida", "in", "nibh", "lorem", "ipsum", "dolor", "sit",
	"amet", "consectetuer", "adipiscing", "elit", "integer", "fermentum", "pretium", "massa", "morbi", "feugiat",
	"iaculis", "nunc", "aenean", "aliquam", "pretium", "orci", "cum", "sociis", "natoque", "penatibus", "et", "magnis",
	"dis", "parturient", "montes", "nascetur", "ridiculus", "mus", "vivamus", "quis", "tellus", "vel", "quam",
	"varius", "bibendum", "fusce", "est", "metus", "feugiat", "at", "porttitor", "et", "cursus", "quis", "pede", "nam", "ut",
	"augue", "nulla", "posuere", "phasellus", "at", "dolor", "a", "enim", "cursus", "vestibulum", "duis", "id", "nisi",
	"duis", "semper", "tellus", "ac", "nulla", "vestibulum", "scelerisque", "lobortis", "dolor", "aenean", "a",
	"felis", "aliquam", "erat", "volutpat", "donec", "a", "magna", "vitae", "pede", "sagittis", "lacinia", "cras",
	"vestibulum", "diam", "ut", "arcu", "mauris", "a", "nunc", "duis", "sollicitudin", "erat", "sit", "amet", "turpis",
	"proin", "at", "libero", "eu", "diam", "lobortis", "fermentum", "nunc", "lorem", "turpis", "imperdiet", "id",
	"gravida", "eget", "aliquet", "sed", "purus", "ut", "vehicula", "laoreet", "ante", "mauris", "eu", "nunc", "sed", "sit",
	"amet", "elit", "nec", "ipsum", "aliquam", "egestas", "donec", "non", "nibh", "cras", "sodales", "pretium", "massa",
	"praesent", "hendrerit", "est", "et", "risus", "vivamus", "eget", "pede", "curabitur", "tristique",
	"scelerisque", "dui", "nullam", "ullamcorper", "vivamus", "venenatis", "velit", "eget", "enim", "nunc", "eu",
	"nunc", "eget", "felis", "malesuada", "fermentum", "quisque", "magna", "mauris", "ligula", "felis", "luctus", "a",
	"aliquet", "nec", "vulputate", "eget", "magna", "quisque", "placerat", "diam", "sed", "arcu", "praesent",
	"sollicitudin", "aliquam", "non", "sapien", "quisque", "id", "augue", "class", "aptent", "taciti", "sociosqu",
	"ad", "litora", "torquent", "per", "conubia", "nostra", "per", "inceptos", "hymenaeos", "etiam", "lacus", "lectus",
	"mollis", "quis", "mattis", "nec", "commodo", "facilisis", "nibh", "sed", "sodales", "sapien", "ac", "ante", "duis",
	"eget", "lectus", "in", "nibh", "lacinia", "auctor", "fusce", "interdum", "lectus", "non", "dui", "integer",
	"accumsan", "quisque", "quam", "curabitur", "scelerisque", "imperdiet", "nisl", "suspendisse", "potenti",
	"nam", "massa", "leo", "iaculis", "sed", "accumsan", "id", "ultrices", "nec", "velit", "suspendisse", "potenti",
	"mauris", "bibendum", "turpis", "ac", "viverra", "sollicitudin", "metus", "massa", "interdum", "orci", "non",
	"imperdiet", "orci", "ante", "at", "ipsum", "etiam", "eget", "magna", "mauris", "at", "tortor", "eu", "lectus",
	"tempor", "tincidunt", "phasellus", "justo", "purus", "pharetra", "ut", "ultricies", "nec", "consequat", "vel",
	"nisi", "fusce", "vitae", "velit", "at", "libero", "sollicitudin", "sodales", "aenean", "mi", "libero", "ultrices",
	"id", "suscipit", "vitae", "dapibus", "eu", "metus", "aenean", "vestibulum", "nibh", "ac", "massa", "vivamus",
	"vestibulum", "libero", "vitae", "purus", "in", "hac", "habitasse", "platea", "dictumst", "curabitur",
	"blandit", "nunc", "non", "arcu", "ut", "nec", "nibh", "morbi", "quis", "leo", "vel", "magna", "commodo", "rhoncus",
	"donec", "congue", "leo", "eu", "lacus", "pellentesque", "at", "erat", "id", "mi", "consequat", "congue", "praesent",
	"a", "nisl", "ut", "diam", "interdum", "molestie", "fusce", "suscipit", "rhoncus", "sem", "donec", "pretium",
	"aliquam", "molestie", "vivamus", "et", "justo", "at", "augue", "aliquet", "dapibus", "pellentesque", "felis",
	"morbi", "semper", "in", "venenatis", "imperdiet", "neque", "donec", "auctor", "molestie", "augue", "nulla", "id",
	"arcu", "sit", "amet", "dui", "lacinia", "convallis", "proin", "tincidunt", "proin", "a", "ante", "nunc", "imperdiet",
	"augue", "nullam", "sit", "amet", "arcu", "quisque", "laoreet", "viverra", "felis", "lorem", "ipsum", "dolor", "sit",
	"amet", "consectetuer", "adipiscing", "elit", "in", "hac", "habitasse", "platea", "dictumst", "pellentesque",
	"habitant", "morbi", "tristique", "senectus", "et", "netus", "et", "malesuada", "fames", "ac", "turpis", "egestas",
	"class", "aptent", "taciti", "sociosqu", "ad", "litora", "torquent", "per", "conubia", "nostra", "per", "inceptos",
	"hymenaeos", "nullam", "nibh", "sapien", "volutpat", "ut", "placerat", "quis", "ornare", "at", "lorem", "class",
	"aptent", "taciti", "sociosqu", "ad", "litora", "torquent", "per", "conubia", "nostra", "per", "inceptos",
	"hymenaeos", "morbi", "dictum", "massa", "id", "libero", "ut", "neque", "phasellus", "tincidunt", "nibh", "ut",
	"tincidunt", "lacinia", "lacus", "nulla", "aliquam", "mi", "a", "interdum", "dui", "augue", "non", "pede", "duis",
	"nunc", "magna", "vulputate", "a", "porta", "at", "tincidunt", "a", "nulla", "praesent", "facilisis",
	"suspendisse", "sodales", "feugiat", "purus", "cras", "et", "justo", "a", "mauris", "mollis", "imperdiet", "morbi",
	"erat", "mi", "ultrices", "eget", "aliquam", "elementum", "iaculis", "id", "velit", "in", "scelerisque", "enim",
	"sit", "amet", "turpis", "sed", "aliquam", "odio", "nonummy", "ullamcorper", "mollis", "lacus", "nibh", "tempor",
	"dolor", "sit", "amet", "varius", "sem", "neque", "ac", "dui", "nunc", "et", "est", "eu", "massa", "eleifend", "mollis",
	"mauris", "aliquet", "orci", "quis", "tellus", "ut", "mattis", "praesent", "mollis", "consectetuer", "quam",
	"nulla", "nulla", "nunc", "accumsan", "nunc", "sit", "amet", "scelerisque", "porttitor", "nibh", "pede", "lacinia",
	"justo", "tristique", "mattis", "purus", "eros", "non", "velit", "aenean", "sagittis", "commodo", "erat",
	"aliquam", "id", "lacus", "morbi", "vulputate", "vestibulum", "elit",
}
