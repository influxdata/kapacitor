package auth

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/influxdata/kapacitor/auth"
	client "github.com/influxdata/kapacitor/client/v1"
	"github.com/influxdata/kapacitor/keyvalue"
	"github.com/influxdata/kapacitor/services/auth/meta"
	"github.com/influxdata/kapacitor/services/httpd"
	"github.com/influxdata/kapacitor/services/storage"
	"github.com/influxdata/kapacitor/tlsconfig"
	"github.com/pkg/errors"

	"golang.org/x/crypto/bcrypt"
)

const (
	usersPath         = "/users"
	usersPathAnchored = "/users/"

	// SaltBytes is the number of bytes used for salts
	saltBytes = 32

	authCacheExpiration = time.Hour
)

type Diagnostic interface {
	Debug(msg string, ctx ...keyvalue.T)
}

type Service struct {
	diag   Diagnostic
	routes []httpd.Route

	StorageService interface {
		Store(namespace string) storage.Interface
	}
	HTTPDService interface {
		AddRoutes([]httpd.Route) error
		DelRoutes([]httpd.Route)
	}

	users           UserDAO
	userCache       UserCache
	cacheExpiration time.Duration

	bcryptCost int

	// Authentication cache.
	// Caches sha256 hashes of passwords for faster authentication
	authCache map[string]authCred
	authMU    sync.RWMutex

	path string

	retentionAutoCreate bool

	// Plutonium Meta Control client
	pmClient *meta.Client
}

type authCred struct {
	salt    []byte
	hash    []byte
	expires time.Time
}

func NewService(c Config, d Diagnostic) (*Service, error) {
	var pmClient *meta.Client
	if c.MetaAddr != "" {
		tlsConfig, err := tlsconfig.Create(c.MetaCA, c.MetaCert, c.MetaKey, c.MetaInsecureSkipVerify)
		if err != nil {
			return nil, errors.Wrap(err, "could not create TLS config for connecting to meta service")
		}
		pmOpts := []meta.ClientOption{
			meta.WithTLS(tlsConfig, c.MetaUseTLS, c.MetaInsecureSkipVerify),
		}
		if c.MetaUsername != "" {
			pmOpts = append(pmOpts, meta.UseAuth(meta.BasicAuth, c.MetaUsername, c.MetaPassword, ""))
		}
		//TODO: when the meta client can accept an interface, pass in a logger
		pmClient = meta.NewClient(c.MetaAddr, pmOpts...)
	} else {
		d.Debug("not using meta service for users, no address given")
	}

	return &Service{
		diag:            d,
		authCache:       make(map[string]authCred),
		cacheExpiration: time.Duration(c.CacheExpiration),
		bcryptCost:      c.BcryptCost,
		pmClient:        pmClient,
	}, nil
}

const userNamespace = "user_store"

func (s *Service) Open() error {
	if s.StorageService == nil {
		return errors.New("missing storage service")
	}
	if s.HTTPDService == nil {
		return errors.New("missing httpd service")
	}
	store := s.StorageService.Store(userNamespace)
	users, err := newUserKV(store)
	if err != nil {
		return err
	}
	s.users = users
	s.userCache = newMemUserCache(s.cacheExpiration)

	// Define API routes
	s.routes = []httpd.Route{
		{
			Method:      "GET",
			Pattern:     usersPathAnchored,
			HandlerFunc: s.handleUser,
		},
		{
			Method:      "DELETE",
			Pattern:     usersPathAnchored,
			HandlerFunc: s.handleDeleteUser,
		},
		{
			// Satisfy CORS checks.
			Method:      "OPTIONS",
			Pattern:     usersPathAnchored,
			HandlerFunc: httpd.ServeOptions,
		},
		{
			Method:      "PATCH",
			Pattern:     usersPathAnchored,
			HandlerFunc: s.handleUpdateUser,
		},
		{
			Method:      "GET",
			Pattern:     usersPath,
			HandlerFunc: s.handleListUsers,
		},
		{
			Method:      "POST",
			Pattern:     usersPath,
			HandlerFunc: s.handleCreateUser,
		},
	}

	if err := s.HTTPDService.AddRoutes(s.routes); err != nil {
		return err
	}
	return nil
}

func (s *Service) Close() error {
	if s.HTTPDService != nil {
		s.HTTPDService.DelRoutes(s.routes)
	}
	return nil
}

func (s *Service) Authenticate(username, password string) (auth.User, error) {
	user, err := s.User(username)
	if err != nil {
		return auth.User{}, err
	}

	// Check for auth cache entry first
	s.authMU.RLock()
	cred, ok := s.authCache[username]
	s.authMU.RUnlock()

	// this happens when influxdb Enterprise is configured to use LDAP, we need to check the password
	// against influxdb
	if len(user.Hash()) == 0 {
		if err := s.pmClient.CheckPass(username, password); err != nil {
			return auth.User{}, errors.Wrap(err, "checking password of Influxdb Enterprise meta user")
		}
		hash, err := bcrypt.GenerateFromPassword([]byte(password), s.bcryptCost)
		if err != nil {
			return auth.User{}, errors.Wrap(err, "failed to hash user password")
		}
		user = auth.NewUser(user.Name(), hash, user.IsAdmin(), user.Privileges())
		s.userCache.Set(user)
	} else if ok {
		// verify the password using the cached salt and hash
		if cred.expires.After(time.Now()) && bytes.Equal(s.hashWithSalt(cred.salt, password), cred.hash) {
			return user, nil
		}
		// fall through to requiring a full bcrypt hash for invalid passwords
	}

	// Compare password with user hash.
	if err := bcrypt.CompareHashAndPassword(user.Hash(), []byte(password)); err != nil {
		s.userCache.Delete(username)
		return auth.User{}, fmt.Errorf("failed to authenticate user")
	}

	// generate a salt and hash of the password for the cache
	if salt, hashed, err := s.saltedHash(password); err == nil {
		s.authMU.Lock()
		s.authCache[username] = authCred{salt: salt, hash: hashed, expires: time.Now().Add(authCacheExpiration)}
		s.authMU.Unlock()
	}
	return user, nil
}

// saltedHash returns a salt and salted hash of password
func (s *Service) saltedHash(password string) (salt, hash []byte, err error) {
	salt = make([]byte, saltBytes)
	if _, err := io.ReadFull(rand.Reader, salt); err != nil {
		return nil, nil, err
	}

	return salt, s.hashWithSalt(salt, password), nil
}

// hashWithSalt returns a salted hash of password using salt
func (s *Service) hashWithSalt(salt []byte, password string) []byte {
	hasher := sha256.New()
	hasher.Write(salt)
	hasher.Write([]byte(password))
	return hasher.Sum(nil)
}

func (s *Service) User(username string) (auth.User, error) {
	// Check cache first
	cached, found := s.userCache.Get(username)
	if found {
		return cached, nil
	}
	// Find the user somewhere
	var user User

	if s.pmClient != nil {
		// Check in InfluxDB Enterprise meta
		pmUsers, err := s.pmClient.Users(username)
		if err != nil {
			return auth.User{}, errors.Wrapf(err, "retrieving user %q from InfluxDB Enterprise meta", username)
		}
		if len(pmUsers) == 1 {
			user, err = s.convertFromPMUser(pmUsers[0])
			if err != nil {
				return auth.User{}, errors.Wrap(err, "converting from InfluxDB Enterprise meta user")
			}
		}
	} else {
		// Check in store
		var err error
		user, err = s.users.Get(username)
		if err != nil {
			return auth.User{}, errors.Wrapf(err, "retrieving user %q from store", username)
		}
	}

	if au, err := s.convertToAuthUser(user); err != nil {
		return auth.User{}, errors.Wrap(err, "converting from stored user")
	} else {
		// Populate cache with user from store
		s.userCache.Set(au)
		return au, nil
	}
}

// Return a user based on the subscription token
func (s *Service) SubscriptionUser(token string) (auth.User, error) {
	username, err := s.subscriptionUsername(token)
	if err != nil {
		return auth.User{}, err
	}
	// Check cache first
	cached, found := s.userCache.Get(username)
	if found {
		return cached, nil
	}

	// Check in store
	user, err := s.users.Get(username)
	if err != nil {
		// The user doesn't exist, so the token is bad.
		return auth.User{}, errors.New("invalid subscription token")
	}

	if au, err := s.convertToAuthUser(user); err != nil {
		return auth.User{}, errors.Wrap(err, "converting from stored user")
	} else {
		// Populate cache with user from store
		s.userCache.Set(au)
		return au, nil
	}
}

const (
	// Prefix used to identfy all users created for granting subscription token access.
	// NOTE: this is technically an invalid username which means that a user with this prefix
	// can't be created externally.
	subscriptionUsernamePrefix = "_sub:"
)

// Convert the token into a subscription username, if valid.
func (s *Service) subscriptionUsername(token string) (string, error) {
	if !validUsername.MatchString(token) {
		return "", fmt.Errorf("token must be a valid username: %q", token)
	}
	return subscriptionUsernamePrefix + token, nil
}

// Convert the username into a subscription token.
func (s *Service) subscriptionToken(username string) string {
	return username[len(subscriptionUsernamePrefix):]
}

func (s *Service) GrantSubscriptionAccess(token, db, rp string) error {
	username, err := s.subscriptionUsername(token)
	if err != nil {
		return err
	}
	dbResource := auth.DatabaseResource(db)
	user := User{
		Name:  username,
		Admin: false,
		Privileges: map[string][]Privilege{
			writeResource: []Privilege{WritePrivilege},
			pingResource:  []Privilege{AllPrivileges},
			dbResource:    []Privilege{WritePrivilege},
		},
	}
	if err := s.users.Create(user); err != nil {
		if err == ErrUserExists {
			if err := s.users.Replace(user); err != nil {
				return errors.Wrap(err, "replacing existing subscription user")
			}
		} else {
			return errors.Wrap(err, "creating subscription user")
		}
	}
	return nil
}

func (s *Service) ListSubscriptionTokens() ([]string, error) {
	var tokens []string
	offset, limit := 0, 100
	pattern := subscriptionUsernamePrefix + "*"
	for {
		users, err := s.users.List(pattern, offset, limit)
		if err != nil {
			return nil, err
		}
		for _, user := range users {
			tokens = append(tokens, s.subscriptionToken(user.Name))
		}

		if len(users) != limit {
			break
		}
		offset += limit
	}
	return tokens, nil
}

func (s *Service) RevokeSubscriptionAccess(token string) error {
	username, err := s.subscriptionUsername(token)
	if err != nil {
		return err
	}
	s.userCache.Delete(username)
	err = s.users.Delete(username)
	return errors.Wrap(err, "deleteing user")
}

// Pattern for valid usernames.
var validUsername = regexp.MustCompile(`^[-\._\p{L}0-9@]+$`)

const usersBasePathAnchored = httpd.BasePath + usersPathAnchored

func (s *Service) usernameFromPath(path string) (string, error) {
	if len(path) <= len(usersBasePathAnchored) {
		return "", errors.New("must specify username on path")
	}
	username := path[len(usersBasePathAnchored):]
	return username, nil
}

func (s *Service) userLink(username string) client.Link {
	return client.Link{Relation: client.Self, Href: path.Join(httpd.BasePath, usersPath, username)}
}

func (s *Service) handleUser(w http.ResponseWriter, r *http.Request) {
	username, err := s.usernameFromPath(r.URL.Path)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusBadRequest)
		return
	}
	u, err := s.users.Get(username)
	if err != nil {
		if err == ErrNoUserExists {
			httpd.HttpError(w, err.Error(), true, http.StatusNotFound)
			return
		}
		httpd.HttpError(w, err.Error(), true, http.StatusInternalServerError)
		return
	}
	cu, err := s.convertToClientUser(u)
	if err != nil {
		httpd.HttpError(w, fmt.Sprintf("failed to convert user: %s", err.Error()), true, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(httpd.MarshalJSON(cu, true))
}

func (s *Service) handleCreateUser(w http.ResponseWriter, r *http.Request) {
	user := client.CreateUserOptions{}
	dec := json.NewDecoder(r.Body)
	err := dec.Decode(&user)
	if err != nil {
		httpd.HttpError(w, "invalid JSON", true, http.StatusBadRequest)
		return
	}
	if user.Name == "" {
		httpd.HttpError(w, "username is required", true, http.StatusBadRequest)
		return
	}
	if !validUsername.MatchString(user.Name) {
		httpd.HttpError(w, fmt.Sprintf("username must contain only letters, numbers, '-', '.', '@' and '_'. %q", user.Name), true, http.StatusBadRequest)
		return
	}
	if user.Password == "" {
		httpd.HttpError(w, "password is required", true, http.StatusBadRequest)
		return
	}

	// Convert Permission to Resources/Privileges
	privileges, err := s.convertPermissions(user.Permissions)
	if err != nil {
		httpd.HttpError(w, fmt.Sprintf("invalid permissions: %s", err.Error()), true, http.StatusBadRequest)
		return
	}

	u, err := s.CreateUser(user.Name, user.Password, user.Type == client.AdminUser, privileges)
	if err != nil {
		httpd.HttpError(w, fmt.Sprintf("failed to create user: %s", err.Error()), true, http.StatusInternalServerError)
		return
	}
	cu, err := s.convertToClientUser(u)
	if err != nil {
		httpd.HttpError(w, fmt.Sprintf("failed to convert user: %s", err.Error()), true, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(httpd.MarshalJSON(cu, true))
}

func (s *Service) handleUpdateUser(w http.ResponseWriter, r *http.Request) {
	username, err := s.usernameFromPath(r.URL.Path)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusBadRequest)
		return
	}
	user := client.UpdateUserOptions{}
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&user); err != nil {
		httpd.HttpError(w, "invalid JSON", true, http.StatusBadRequest)
		return
	}

	// If user.Permissions is nil then the client didn't list any so don't update permissions.
	updatePrivileges := user.Permissions != nil

	// Convert Permission to Resources/Privileges
	privileges, err := s.convertPermissions(user.Permissions)
	if err != nil {
		httpd.HttpError(w, fmt.Sprintf("invalid permissions: %s", err.Error()), true, http.StatusBadRequest)
		return
	}

	u, err := s.updateUser(
		username,
		user.Password,
		user.Type != client.InvalidUser,
		user.Type == client.AdminUser,
		updatePrivileges,
		privileges,
	)
	if err != nil {
		httpd.HttpError(w, fmt.Sprintf("failed to create user: %s", err.Error()), true, http.StatusInternalServerError)
		return
	}
	cu, err := s.convertToClientUser(u)
	if err != nil {
		httpd.HttpError(w, fmt.Sprintf("failed to convert user: %s", err.Error()), true, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(httpd.MarshalJSON(cu, true))
}

func (s *Service) handleDeleteUser(w http.ResponseWriter, r *http.Request) {
	username, err := s.usernameFromPath(r.URL.Path)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusBadRequest)
		return
	}

	s.userCache.Delete(username)

	s.authMU.Lock()
	delete(s.authCache, username)
	s.authMU.Unlock()

	if err := s.users.Delete(username); err != nil {
		httpd.HttpError(w, fmt.Sprintf("failed to delete user: %s", err.Error()), true, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

var allUserFields = []string{
	"link",
	"name",
	"type",
	"permissions",
}

func (s *Service) handleListUsers(w http.ResponseWriter, r *http.Request) {
	pattern := r.URL.Query().Get("pattern")
	fields := r.URL.Query()["fields"]
	if len(fields) == 0 {
		fields = allUserFields
	} else {
		// Always return ID field
		fields = append(fields, "name", "link")
	}

	var err error
	offset := int64(0)
	offsetStr := r.URL.Query().Get("offset")
	if offsetStr != "" {
		offset, err = strconv.ParseInt(offsetStr, 10, 64)
		if err != nil {
			httpd.HttpError(w, fmt.Sprintf("invalid offset parameter %q must be an integer: %s", offsetStr, err), true, http.StatusBadRequest)
		}
	}

	limit := int64(100)
	limitStr := r.URL.Query().Get("limit")
	if limitStr != "" {
		limit, err = strconv.ParseInt(limitStr, 10, 64)
		if err != nil {
			httpd.HttpError(w, fmt.Sprintf("invalid limit parameter %q must be an integer: %s", limitStr, err), true, http.StatusBadRequest)
		}
	}

	rawUsers, err := s.users.List(pattern, int(offset), int(limit))
	users := make([]map[string]interface{}, len(rawUsers))

	for i, user := range rawUsers {
		users[i] = make(map[string]interface{}, len(fields))
		for _, field := range fields {
			var value interface{}
			switch field {
			case "name":
				value = user.Name
			case "link":
				value = s.userLink(user.Name)
			case "type":
				value = client.NormalUser
				if user.Admin {
					value = client.AdminUser
				}
			case "permissions":
				value = s.convertPrivleges(user.Privileges)
			default:
				httpd.HttpError(w, fmt.Sprintf("unsupported field %q", field), true, http.StatusBadRequest)
				return
			}
			users[i][field] = value
		}
	}

	type response struct {
		Users []map[string]interface{} `json:"users"`
	}

	w.Write(httpd.MarshalJSON(response{users}, true))
}

var (
	rootResource   = auth.APIResource("/")
	writeResource  = auth.APIResource("/write")
	pingResource   = auth.APIResource("/ping")
	configResource = auth.APIResource("/config")
)

// Map permissions to a set of resources and privileges
func (s *Service) convertPermissions(perms []client.Permission) (map[string][]Privilege, error) {
	privileges := make(map[string][]Privilege, len(perms))
	for _, perm := range perms {
		switch perm {
		case client.NoPermissions:
		case client.APIPermission:
			privileges[rootResource] = []Privilege{AllPrivileges}
			// Subtractive permission, only add it if something else doesn't already exist.
			if _, ok := privileges[writeResource]; !ok {
				privileges[writeResource] = []Privilege{NoPrivileges}
			}
			// Do not give config API access unless specificaly granted
			if _, ok := privileges[configResource]; !ok {
				privileges[configResource] = []Privilege{NoPrivileges}
			}
		case client.ConfigAPIPermission:
			privileges[pingResource] = []Privilege{AllPrivileges}
			privileges[configResource] = []Privilege{AllPrivileges}
		case client.WritePointsPermission:
			privileges[pingResource] = []Privilege{AllPrivileges}
			privileges[writeResource] = []Privilege{AllPrivileges}
		case client.AllPermissions:
			privileges[rootResource] = []Privilege{AllPrivileges}
			privileges[configResource] = []Privilege{AllPrivileges}
			privileges[writeResource] = []Privilege{AllPrivileges}
		default:
			return nil, fmt.Errorf("unknown permission %v", perm)
		}
	}
	return privileges, nil
}

// Map a set of resources and privileges to permissions
func (s *Service) convertPrivleges(privileges map[string][]Privilege) []client.Permission {
	hasAPI := false
	if ps, ok := privileges[rootResource]; ok {
		for _, p := range ps {
			if p == AllPrivileges {
				hasAPI = true
			}
		}
	}
	hasConfig := false
	if ps, ok := privileges[configResource]; ok {
		for _, p := range ps {
			if p == AllPrivileges {
				hasConfig = true
			}
		}
	}
	hasWrite := false
	if ps, ok := privileges[writeResource]; ok {
		for _, p := range ps {
			if p == AllPrivileges {
				hasWrite = true
			}
		}
	}
	if hasAPI && hasWrite && hasConfig {
		return []client.Permission{client.AllPermissions}
	}
	perms := make([]client.Permission, 0)
	if hasAPI {
		perms = append(perms, client.APIPermission)
	}
	if hasConfig {
		perms = append(perms, client.ConfigAPIPermission)
	}
	if hasWrite {
		perms = append(perms, client.WritePointsPermission)
	}
	return perms
}

// Map a set of InfluxDB Enterprise meta permissions to a set of privileges.
func convertPMPermissions(scopedPerms map[string][]meta.Permission) map[string][]Privilege {
	privileges := make(map[string][]Privilege, len(scopedPerms))
	for scope, perms := range scopedPerms {
		dbResource := auth.DatabaseResource(scope)
		for _, perm := range perms {
			switch perm {
			case meta.KapacitorAPIPermission:
				privileges[rootResource] = []Privilege{AllPrivileges}
				// Do not give config API access unless specifically granted
				if _, ok := privileges[configResource]; !ok {
					privileges[configResource] = []Privilege{NoPrivileges}
				}
			case meta.KapacitorConfigAPIPermission:
				privileges[pingResource] = []Privilege{AllPrivileges}
				privileges[configResource] = []Privilege{AllPrivileges}
			case meta.WriteDataPermission:
				privileges[dbResource] = append(privileges[dbResource], WritePrivilege)
				privileges[writeResource] = []Privilege{WritePrivilege}
				privileges[pingResource] = []Privilege{AllPrivileges}
			case meta.ReadDataPermission:
				privileges[pingResource] = []Privilege{AllPrivileges}
				privileges[dbResource] = append(privileges[dbResource], ReadPrivilege)
			default:
				// Ignore, user can have permissions for InfluxDB related actions.
			}
		}
	}
	return privileges
}

func (s *Service) CreateUser(username, password string, admin bool, privileges map[string][]Privilege) (User, error) {
	// Check if user exists
	_, err := s.users.Get(username)
	if err != ErrNoUserExists {
		return User{}, errors.Wrap(err, "creating user")
	}

	// Hash the password before serializing it.
	hash, err := bcrypt.GenerateFromPassword([]byte(password), s.bcryptCost)
	if err != nil {
		return User{}, errors.Wrap(err, "hashing password")
	}
	u := User{
		Name:       username,
		Hash:       hash,
		Admin:      admin,
		Privileges: privileges,
	}

	if err := s.users.Create(u); err != nil {
		return User{}, errors.Wrap(err, "saving user")
	}
	// Populate user cache
	if au, err := s.convertToAuthUser(u); err == nil {
		s.userCache.Set(au)
	}
	return u, nil
}

func (s *Service) updateUser(username, password string, updateAdmin, admin, updatePrivileges bool, privileges map[string][]Privilege) (User, error) {
	// Check if user exists
	u, err := s.users.Get(username)
	if err != nil {
		return User{}, errors.Wrap(err, "updating user")
	}

	if password != "" {
		// Hash the password before serializing it.
		hash, err := bcrypt.GenerateFromPassword([]byte(password), s.bcryptCost)
		if err != nil {
			return User{}, errors.Wrap(err, "hashing password")
		}
		u.Hash = hash
	}

	if updatePrivileges {
		u.Privileges = privileges
	}
	if updateAdmin {
		u.Admin = admin
		if admin {
			// Zero user privileges since now its an admin.
			u.Privileges = nil
		}
	}

	if err := s.users.Replace(u); err != nil {
		return User{}, errors.Wrap(err, "saving user")
	}
	// Populate user cache
	if au, err := s.convertToAuthUser(u); err == nil {
		s.userCache.Set(au)
	}
	return u, nil
}

func (s *Service) DeleteUser(username string) {
	s.users.Delete(username)
	s.userCache.Delete(username)
}

// Convert a user from the store into an auth.User.
func (s *Service) convertToAuthUser(u User) (auth.User, error) {
	privileges := make(map[string][]auth.Privilege, len(u.Privileges))
	for r, ps := range u.Privileges {
		for _, p := range ps {
			var priv auth.Privilege
			switch p {
			case NoPrivileges:
				priv = auth.NoPrivileges
			case ReadPrivilege:
				priv = auth.ReadPrivilege
			case WritePrivilege:
				priv = auth.WritePrivilege
			case DeletePrivilege:
				priv = auth.DeletePrivilege
			case AllPrivileges:
				priv = auth.AllPrivileges
			default:
				return auth.User{}, fmt.Errorf("unknown Privilege %v", p)
			}
			privileges[r] = append(privileges[r], priv)
		}
	}
	au := auth.NewUser(u.Name, u.Hash, u.Admin, privileges)
	return au, nil
}

// Convert an auth.User into a user for the store.
func (s *Service) convertFromAuthUser(au auth.User) (User, error) {
	u := User{
		Name:  au.Name(),
		Admin: au.IsAdmin(),
		Hash:  au.Hash(),
	}
	if !u.Admin {
		privileges := au.Privileges()
		u.Privileges = make(map[string][]Privilege, len(privileges))
		for r, ps := range privileges {
			for _, p := range ps {
				var priv Privilege
				switch p {
				case auth.NoPrivileges:
					priv = NoPrivileges
				case auth.ReadPrivilege:
					priv = ReadPrivilege
				case auth.WritePrivilege:
					priv = WritePrivilege
				case auth.DeletePrivilege:
					priv = DeletePrivilege
				case auth.AllPrivileges:
					priv = AllPrivileges
				default:
					return User{}, fmt.Errorf("unknown auth.Privilege %v", p)
				}
				u.Privileges[r] = append(u.Privileges[r], priv)
			}
		}
	}
	return u, nil
}

// Convert a meta.User into a user for the store.
func (s *Service) convertFromPMUser(mu meta.User) (User, error) {
	u := User{
		Name:       mu.Name,
		Hash:       []byte(mu.Hash),
		Privileges: convertPMPermissions(mu.Permissions),
	}
	return u, nil
}

// Convert a user from the store into an client.User.
func (s *Service) convertToClientUser(u User) (client.User, error) {
	ut := client.NormalUser
	if u.Admin {
		ut = client.AdminUser
	}
	perms := s.convertPrivleges(u.Privileges)
	return client.User{
		Link:        s.userLink(u.Name),
		Name:        u.Name,
		Type:        ut,
		Permissions: perms,
	}, nil
}
