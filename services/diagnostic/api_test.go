package diagnostic

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sync"
	"testing"
)

type mockSessionsStore struct {
	wg      sync.WaitGroup
	session *Session
}

func (h *mockSessionsStore) Create(w http.ResponseWriter, contentType string, level Level, tags []tag) *Session {
	h.session = &Session{
		level:       level,
		tags:        tags,
		contentType: contentType,
	}
	h.wg.Done()

	return h.session
}

func (h *mockSessionsStore) Delete(s *Session) error {
	return nil
}

func (h *mockSessionsStore) Each(func(*Session)) {
}

func (h *mockSessionsStore) SetDiagnostic(Diagnostic) {
}

func NewMockedSessionService() (*SessionService, *mockSessionsStore) {
	m := &mockSessionsStore{}
	m.wg.Add(1)
	return &SessionService{
		SessionsStore: m,
	}, m
}

func TestHandleSessions_level(t *testing.T) {
	tests := []struct {
		name        string
		expLevel    Level
		expTags     []tag
		queryParams map[string]string
	}{
		{
			name:     "Static error pair",
			expLevel: DebugLevel,
			expTags:  []tag{{"lvl", "error"}},
			queryParams: map[string]string{
				"lvl": "error",
			},
		},
		{
			name:     "error level",
			expLevel: ErrorLevel,
			queryParams: map[string]string{
				"lvl": "error+",
			},
		},
		{
			name:     "info level",
			expLevel: InfoLevel,
			queryParams: map[string]string{
				"lvl": "info+",
			},
		},
		{
			name:     "info level with extra tags",
			expLevel: InfoLevel,
			expTags:  []tag{{"cool", "tag"}},
			queryParams: map[string]string{
				"lvl":  "info+",
				"cool": "tag",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, m := NewMockedSessionService()
			u := "http://localhost:9092/kapacitor/v1/sessions"
			values := url.Values{}
			for k, v := range tt.queryParams {
				values.Add(k, v)
			}
			w := httptest.NewRecorder()
			r := httptest.NewRequest("GET", u+"?"+values.Encode(), nil)

			go func() {
				s.handleSessions(w, r)
			}()
			m.wg.Wait()

			if exp, got := tt.expLevel, m.session.level; exp != got {
				t.Fatalf("Bad level, expected %v got %v", exp, got)
			}

			// sort tags
			if exp, got := tt.expTags, m.session.tags; exp != nil && got != nil && !reflect.DeepEqual(exp, got) {
				t.Fatalf("Bad tags, expected %v got %v", exp, got)
			}
		})
	}

}
