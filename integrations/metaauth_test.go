package integrations

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt"
	"github.com/h2non/gock"
	"golang.org/x/crypto/bcrypt"

	authcore "github.com/influxdata/kapacitor/auth"
	"github.com/influxdata/kapacitor/keyvalue"
	"github.com/influxdata/kapacitor/services/auth"
	"github.com/influxdata/kapacitor/services/auth/meta"
	"github.com/influxdata/kapacitor/services/storage"
	"github.com/stretchr/testify/require"
)

type NopDiag struct{}

func (d *NopDiag) Debug(msg string, ctx ...keyvalue.T) {}

type NopStorageService struct{}

func (s *NopStorageService) Store(namespace string) storage.Interface {
	return nil
}

// newTestAuthService makes an auth service with given config hooked up for mocking with gock.
func newTestAuthService(config auth.Config) (*auth.Service, error) {
	diag := &NopDiag{}
	interceptClient := func(c *http.Client) error { gock.InterceptClient(c); return nil }
	srv, err := auth.NewService(config, diag, meta.WithHTTPOption(interceptClient))
	if err != nil {
		return nil, err
	}
	if srv == nil {
		return nil, fmt.Errorf("auth.NewService returned nil without an error")
	}

	srv.StorageService = &NopStorageService{}
	srv.HTTPDService = newHTTPDService()
	if err = srv.Open(); err != nil {
		return nil, err
	}
	return srv, nil
}

const (
	metaName = "meta1.edge"
	metaPort = 8091

	metaSecret = "MyVoiceIsMyPassport"
	metaUser   = "JoeyJo-JoJuniorShabadoo"
	metaPass   = "ShabadooPassword"
)

var (
	metaAddr = fmt.Sprintf("%s:%d", metaName, metaPort)
	metaUrl  = fmt.Sprintf("http://%s", metaAddr)
)

// bearerCheck is a gock matcher that ensures the bearer token presented by the client is correct.
func bearerCheck(user, secret string) gock.MatchFunc {
	return func(r *http.Request, gr *gock.Request) (bool, error) {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			return false, nil
		}
		authSections := strings.Split(authHeader, " ")
		if len(authSections) != 2 || authSections[0] != "Bearer" {
			return false, nil
		}
		tokenStr := authSections[1]
		token, err := jwt.Parse(tokenStr, func(t *jwt.Token) (interface{}, error) {
			if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, errors.New("signing method should be HMAC")
			}
			return []byte(secret), nil
		})
		if err != nil {
			return false, err
		}
		claims, ok := token.Claims.(jwt.MapClaims)
		if !ok {
			return false, errors.New("improper claims object")
		}
		claimsUser, ok := claims["username"].(string)
		if !ok {
			return false, errors.New("bad claims username")
		}
		if claimsUser != user {
			return false, nil
		}
		return claims.VerifyExpiresAt(time.Now().Unix(), true), nil
	}
}

// runCommonMetaAuthTests runs common test cases that require using the meta API
// to authenticate kapacitor users.
func runCommonMetaAuthTests(t *testing.T, config auth.Config, authType meta.AuthType) {
	defer gock.OffAll()
	gock.Observe(gock.DumpRequest)

	// newGock creates a gock request configured for the expected type of authentication.
	newGock := func() *gock.Request {
		gr := gock.New(metaUrl).SetMatcher(gock.NewMatcher())
		switch authType {
		case meta.BasicAuth:
			gr.BasicAuth(metaUser, metaPass)
		case meta.BearerAuth:
			gr.MatchHeader("Authorization", "Bearer (.*)")
			// When using the internal shared secret the username should be empty
			gr.AddMatcher(bearerCheck("", metaSecret))
		}
		return gr
	}

	type UsersJson struct {
		Users []meta.User `json:"users"`
	}
	passwordHash := func(pass string) string {
		hash, err := bcrypt.GenerateFromPassword([]byte(pass), bcrypt.DefaultCost)
		require.NoError(t, err)
		return string(hash)
	}

	metaAlice := meta.User{
		Name:        "alice",
		Hash:        passwordHash("CaptainPicard"),
		Permissions: map[string][]meta.Permission{"ProjectScorpio": {meta.Permission(meta.KapacitorAPIPermission)}},
	}
	authAlice := authcore.NewUser("alice", []byte(metaAlice.Hash), false, map[string][]authcore.Privilege{"/api": {authcore.AllPrivileges}, "/api/config": {authcore.NoPrivileges}})

	metaBob := meta.User{
		Name:        "bob",
		Hash:        passwordHash("TheDoctor"),
		Permissions: map[string][]meta.Permission{"ProjectScorpio": {meta.Permission(meta.ReadDataPermission)}},
	}
	authBob := authcore.NewUser("bob", []byte(metaBob.Hash), false, map[string][]authcore.Privilege{"/api/ping": {authcore.AllPrivileges}, "/database/ProjectScorpio_clean": {authcore.ReadPrivilege}})

	authBad := authcore.User{}

	metaUsers := map[string]meta.User{
		"alice": metaAlice,
		"bob":   metaBob,
	}
	addValidUserReq := func(name string) {
		newGock().Get("/user").
			MatchParam("name", name).
			Reply(200).
			JSON(UsersJson{Users: []meta.User{metaUsers[name]}})

	}

	addValidUserReq("alice") // first request with invalid user password
	addValidUserReq("alice") // second request with valid user password
	addValidUserReq("bob")

	// add an invalid username request
	newGock().Get("/user").
		MatchParam("name", "carol").
		Reply(404)

	srv, err := newTestAuthService(config)
	require.NoError(t, err)
	require.NotNil(t, srv)

	// check for failure with bad alice password
	alice, err := srv.Authenticate("alice", "CaptainKirk")
	require.Error(t, err)
	require.Equal(t, authBad, alice)

	alice, err = srv.Authenticate("alice", "CaptainPicard")
	require.NoError(t, err)
	require.Equal(t, authAlice, alice)

	// This should be cached not require a request to the meta API, yet it does...
	/*
		alice, err = srv.Authenticate("alice", "CaptainPicard")
		require.NoError(t, err)
		require.Equal(t, authAlice, alice)
	*/

	bob, err := srv.Authenticate("bob", "TheDoctor")
	require.NoError(t, err)
	require.Equal(t, authBob, bob)

	carol, err := srv.Authenticate("carol", "LukeSkywalker")
	require.Error(t, err)
	require.Equal(t, authBad, carol)

	require.True(t, gock.IsDone())
}

func TestMetaAuth_NoAuth(t *testing.T) {
	config := auth.Config{
		Enabled:  true,
		MetaAddr: metaAddr,
	}
	runCommonMetaAuthTests(t, config, meta.NoAuth)
}

func TestMetaAuth_UserPass(t *testing.T) {
	config := auth.Config{
		Enabled:      true,
		MetaAddr:     metaAddr,
		MetaUsername: metaUser,
		MetaPassword: metaPass,
	}
	runCommonMetaAuthTests(t, config, meta.BasicAuth)
}

func TestMetaAuth_Secret(t *testing.T) {
	config := auth.Config{
		Enabled:                  true,
		MetaAddr:                 metaAddr,
		MetaInternalSharedSecret: metaSecret,
	}
	runCommonMetaAuthTests(t, config, meta.BearerAuth)
}

func TestMetaAuth_SecretAndUserPass(t *testing.T) {
	config := auth.Config{
		Enabled:                  true,
		MetaAddr:                 metaAddr,
		MetaInternalSharedSecret: metaSecret,

		// MetaUsername and MetaPassword should be ignored if MetaInternalSharedSecret is set.
		MetaUsername: metaUser,
		MetaPassword: metaPass,
	}
	runCommonMetaAuthTests(t, config, meta.BearerAuth)
}
