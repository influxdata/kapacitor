package noauth

import (
	"log"

	"github.com/influxdata/kapacitor/auth"
)

// Provide an implentation of an Authentication service.
// NOTE: This service provides no real authentication but rather
// returns admin users for all requests.
type Service struct {
	logger *log.Logger
}

func NewService(l *log.Logger) *Service {
	return &Service{
		logger: l,
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
	s.logger.Println("W! using noauth auth backend. Faked authentication for user", username)
	return auth.NewAdminUser(username), nil
}
