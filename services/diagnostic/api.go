package diagnostic

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/influxdata/kapacitor/services/httpd"
	"github.com/influxdata/kapacitor/uuid"
)

const (
	sessionsPath = "/logs"
)

type Diagnostic interface {
	CreatedLogSession(id uuid.UUID, contentType string, tags []tag)
	DeletedLogSession(id uuid.UUID, contentType string, tags []tag)
}

type SessionService struct {
	diag   Diagnostic
	routes []httpd.Route

	SessionsStore SessionsStore
	HTTPDService  interface {
		AddPreviewRoutes([]httpd.Route) error
		DelRoutes([]httpd.Route)
	}
}

func NewSessionService() *SessionService {
	return &SessionService{
		SessionsStore: &sessionsStore{
			sessions: make(map[uuid.UUID]*Session),
		},
	}
}

func (s *SessionService) SetDiagnostic(d Diagnostic) {
	s.SessionsStore.SetDiagnostic(d)
}

func (s *SessionService) Close() error {
	if s.HTTPDService == nil {
		return errors.New("must set HTTPDService")
	}

	s.HTTPDService.DelRoutes(s.routes)
	return nil
}

func (s *SessionService) Open() error {

	s.routes = []httpd.Route{
		{
			Method:      "GET",
			Pattern:     sessionsPath,
			HandlerFunc: s.handleSessions,
			// NoGzip is true so that clients don't have to specify
			// the header "Accept-Encoding: identity"
			NoGzip: true,
			// Data returned is not necessarily JSON. Server
			// sets "Content-Type" appropriately.
			NoJSON: true,
		},
	}

	if s.HTTPDService == nil {
		return errors.New("must set HTTPDService")
	}

	if err := s.HTTPDService.AddPreviewRoutes(s.routes); err != nil {
		return fmt.Errorf("failed to add routes: %v", err)
	}
	return nil
}

func (s *SessionService) NewLogger() *sessionsLogger {
	return &sessionsLogger{
		store: s.SessionsStore,
	}
}

func (s *SessionService) handleSessions(w http.ResponseWriter, r *http.Request) {
	params := r.URL.Query()
	tags := []tag{}
	var level Level

	for k, v := range params {
		if len(v) != 1 {
			httpd.HttpError(w, "query params cannot contain duplicate params", true, http.StatusBadRequest)
			return
		}

		if k == "lvl" && strings.HasSuffix(v[0], "+") {
			level = logLevelFromName(strings.TrimSuffix(v[0], "+"))
			continue
		}

		tags = append(tags, tag{key: k, value: v[0]})
	}

	acceptedContentType := r.Header.Get("Accept")
	var contentType string
	switch acceptedContentType {
	case "application/json":
		contentType = acceptedContentType
	default:
		contentType = "application/logfmt"
	}

	header := w.Header()
	header.Add("Transfer-Encoding", "chunked")
	header.Add("Content-Type", contentType)
	w.WriteHeader(http.StatusOK)

	session := s.SessionsStore.Create(w, contentType, level, tags)
	defer s.SessionsStore.Delete(session)

	<-r.Context().Done()
}
