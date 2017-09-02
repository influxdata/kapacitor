package noauth

import (
	"github.com/influxdata/kapacitor/auth"
	"github.com/influxdata/kapacitor/services/diagnostic"
)

// Provide an implentation of an Authentication service.
// NOTE: This service provides no real authentication but rather
// returns admin users for all requests.
type Service struct {
	diagnostic diagnostic.Diagnostic
}

func NewService(d diagnostic.Diagnostic) *Service {
	return &Service{
		diagnostic: d,
	}
}

func (s *Service) Open() error {
	return nil
}
func (s *Service) Close() error {
	return nil
}

// Return a user will all privileges and given username.
// NOTE: Password is ignored as no real authentication is performed.
func (s *Service) Authenticate(username, password string) (auth.User, error) {
	return s.User(username)
}

// Return a user will all privileges and given username.
func (s *Service) User(username string) (auth.User, error) {
	s.diagnostic.Diag(
		"level", "warn",
		"msg", "using noauth auth backend. Faked authentication for user",
		"user", username,
	)

	return auth.NewUser(username, nil, true, nil), nil
}

// Return a user will all privileges.
func (s *Service) SubscriptionUser(token string) (auth.User, error) {
	s.diagnostic.Diag(
		"level", "warn",
		"msg", "using noauth auth backend. Faked authentication for subscription user token",
	)
	return auth.NewUser("subscription-user", nil, true, nil), nil
}

func (s *Service) GrantSubscriptionAccess(token, db, rp string) error {
	return nil
}

func (s *Service) ListSubscriptionTokens() ([]string, error) {
	return nil, nil
}

func (s *Service) RevokeSubscriptionAccess(token string) error {
	return nil
}
