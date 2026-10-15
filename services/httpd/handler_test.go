package httpd

import (
	"expvar"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/influxdata/kapacitor/auth"
)

func Test_RequiredPrilegeForHTTPMethod(t *testing.T) {
	testCases := []struct {
		m   string
		rp  auth.Privilege
		err error
	}{
		{
			m:   "GET",
			rp:  auth.ReadPrivilege,
			err: nil,
		},
		{
			m:   "get",
			rp:  auth.ReadPrivilege,
			err: nil,
		},
		{
			m:   "HEAD",
			rp:  auth.NoPrivileges,
			err: nil,
		},
		{
			m:   "head",
			rp:  auth.NoPrivileges,
			err: nil,
		},
		{
			m:   "OPTIONS",
			rp:  auth.NoPrivileges,
			err: nil,
		},
		{
			m:   "options",
			rp:  auth.NoPrivileges,
			err: nil,
		},
		{
			m:   "POST",
			rp:  auth.WritePrivilege,
			err: nil,
		},
		{
			m:   "post",
			rp:  auth.WritePrivilege,
			err: nil,
		},
		{
			m:   "PATCH",
			rp:  auth.WritePrivilege,
			err: nil,
		},
		{
			m:   "patch",
			rp:  auth.WritePrivilege,
			err: nil,
		},
		{
			m:   "DELETE",
			rp:  auth.DeletePrivilege,
			err: nil,
		},
		{
			m:   "delete",
			rp:  auth.DeletePrivilege,
			err: nil,
		},
		{
			m:   "PUT",
			rp:  auth.WritePrivilege,
			err: nil,
		},
		{
			m:   "put",
			rp:  auth.WritePrivilege,
			err: nil,
		},
	}

	for _, tc := range testCases {
		got, err := requiredPrivilegeForHTTPMethod(tc.m)
		if err != nil {
			if tc.err == nil {
				t.Errorf("unexpected error: got %v", err)
			} else if tc.err.Error() != err.Error() {
				t.Errorf("unexpected error message: got %q exp %q", err.Error(), tc.err.Error())
			}
		} else {
			if tc.err != nil {
				t.Errorf("expected error: %q got nil", tc.err.Error())
				continue
			}
			if got != tc.rp {
				t.Errorf("unexpected required privilege: got %v exp %v", got, tc.rp)
			}
		}
	}
}

// TestAuthenticate_BearerSharedSecret checks the guard that rejects bearer
// authentication when the shared secret is empty.
func TestAuthenticate_BearerSharedSecret(t *testing.T) {
	cases := []struct {
		name         string
		sharedSecret string
		wantStatus   int
		wantBody     string
		wantAuthFail int64
	}{
		{
			name:         "empty shared secret rejects bearer before parsing",
			sharedSecret: "",
			wantStatus:   http.StatusUnauthorized,
			wantBody:     "bearer authentication is not enabled",
			wantAuthFail: 1,
		},
		{
			// The parser rejects this token. That path does not count an
			// auth failure, so the counter stays at 0.
			name:         "configured shared secret still parses the token",
			sharedSecret: "s3cr3t",
			wantStatus:   http.StatusUnauthorized,
			wantBody:     "invalid token",
			wantAuthFail: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := &Handler{
				sharedSecret: tc.sharedSecret,
				statMap:      (&expvar.Map{}).Init(),
			}

			innerCalled := false
			inner := func(w http.ResponseWriter, r *http.Request, user auth.User) {
				innerCalled = true
			}

			req := httptest.NewRequest("GET", "/kapacitor/v1/tasks", nil)
			// Any token string reaches the bearer branch. For the empty-secret
			// case the guard rejects it before parsing, so the value does not
			// matter. For the configured-secret case this value is not a valid
			// token, so the parser rejects it.
			req.Header.Set("Authorization", "Bearer placeholder")

			rec := httptest.NewRecorder()
			authenticate(inner, h, true).ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Errorf("status: got %d exp %d", rec.Code, tc.wantStatus)
			}
			if innerCalled {
				t.Error("inner handler ran; the request must be rejected")
			}
			if body := rec.Body.String(); !strings.Contains(body, tc.wantBody) {
				t.Errorf("body: got %q want substring %q", body, tc.wantBody)
			}

			var authFail int64
			if v := h.statMap.Get(statAuthFail); v != nil {
				if iv, ok := v.(*expvar.Int); ok {
					authFail = iv.Value()
				}
			}
			if authFail != tc.wantAuthFail {
				t.Errorf("auth_fail count: got %d exp %d", authFail, tc.wantAuthFail)
			}
		})
	}
}
