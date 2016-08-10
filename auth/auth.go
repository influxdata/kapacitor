package auth

import (
	"fmt"
	"path"
	"strings"
)

// Interface for authenticating and retrieving users.
type Interface interface {
	Authenticate(username, password string) (User, error)
	User(username string) (User, error)
	SubscriptionUser(token string) (User, error)
	GrantSubscriptionAccess(token, db, rp string) error
	ListSubscriptionTokens() ([]string, error)
	RevokeSubscriptionAccess(token string) error
}
type Privilege uint

const (
	NoPrivileges Privilege = 1 << iota

	ReadPrivilege
	WritePrivilege
	DeletePrivilege

	AllPrivileges
)

var PrivilegeList []Privilege

func init() {
	p := Privilege(0)
	for i := uint(0); p < AllPrivileges; i++ {
		p = 1 << i
		PrivilegeList = append(PrivilegeList, p)
	}
}

func (p Privilege) String() string {
	switch p {
	case NoPrivileges:
		return "none"
	case ReadPrivilege:
		return "read"
	case WritePrivilege:
		return "write"
	case DeletePrivilege:
		return "delete"
	case AllPrivileges:
		return "all"
	default:
		return "unknown"
	}
}

type Action struct {
	Resource  string
	Privilege Privilege
}

// This structure is designed to be immutable, to avoid bugs/exploits where
// the user could be modified by external code.
// For this reason all fields are private and methods are value receivers.
type User struct {
	name  string
	admin bool
	hash  []byte
	// Map of resource -> Bitmask of Privileges
	privileges map[string]Privilege
}

// Create a user with the given privileges.
func NewUser(name string, hash []byte, admin bool, privileges map[string][]Privilege) User {
	ps := make(map[string]Privilege, len(privileges))
	// Clean resources and convert to bitmask
	for resource, privileges := range privileges {
		clean := path.Clean(resource)
		mask := Privilege(0)
		for _, p := range privileges {
			mask |= p
		}
		ps[clean] = mask
	}
	// Make our own copy of the hash
	h := make([]byte, len(hash))
	copy(h, hash)
	return User{
		name:       name,
		admin:      admin,
		hash:       h,
		privileges: ps,
	}
}

// This user has all privileges for all resources.
var AdminUser = NewUser("ADMIN_USER", nil, true, nil)

// Determine wether the user is authorized to take the action.
func (u User) Name() string {
	return u.name
}

// Report whether the user is an Admin user
func (u User) IsAdmin() bool {
	return u.admin
}

// Return a copy of the user's password hash
func (u User) Hash() []byte {
	hash := make([]byte, len(u.hash))
	copy(hash, u.hash)
	return hash
}

// Return a copy of the privileges the user has.
func (u User) Privileges() map[string][]Privilege {
	privileges := make(map[string][]Privilege)
	for r, ps := range u.privileges {
		for _, p := range PrivilegeList {
			if ps&p != 0 {
				privileges[r] = append(privileges[r], p)
			}
		}
	}
	return privileges
}

// Determine wether the user is authorized to take the action.
// Returns nil if the action is authorized, otherwise returns an error describing the needed permissions.
func (u User) AuthorizeAction(action Action) error {
	if action.Privilege == NoPrivileges || u.admin {
		return nil
	}
	// Find a matching resource of the form /path/to/resource
	// where if the resource is /a/b/c and the user has permision to /a/b
	// then it is considered valid.
	if !path.IsAbs(action.Resource) {
		return fmt.Errorf("invalid action resource: %q, must be an absolute path", action.Resource)
	}
	if len(u.privileges) > 0 {
		// Clean path to prevent path traversal like /a/b/../d when user has access to /a/b
		resource := path.Clean(action.Resource)
		for {
			if p, ok := u.privileges[resource]; ok {
				// Found matching resource
				authorized := p&action.Privilege != 0 || p == AllPrivileges
				if authorized {
					return nil
				} else {
					break
				}
			}
			if resource == "/" {
				break
			}
			// Pop off the last piece of the resource and try again
			resource = path.Dir(resource)
		}
	}
	return authError{
		username: u.name,
		action:   action,
	}
}

// All auth errors are of this type.
type authError struct {
	username string
	action   Action
}

func (e authError) Error() string {
	return fmt.Sprintf("user %s does not have \"%v\" privilege for resource %q", e.username, e.action.Privilege, e.action.Resource)
}

func (e authError) MissingPrivlege() Privilege {
	return e.action.Privilege
}

const (
	databaseRootResource = "/database"
	apiRootResource      = "/api"

	cleanSuffix = "_clean"
	dirtySuffix = "_dirty"
)

func APIResource(p string) string {
	return path.Join(apiRootResource, p)
}

func DatabaseResource(database string) string {
	// Map the database name to a path element name.
	//  1. Replace all '/' with '_'.
	//  2. Mark whether the database name was clean or dirty.
	//
	// This transformation has two important properties:
	//  1. The resulting name will be considered a single path element.
	//  2. The transformation is a one-to-one function.
	//
	// Examples:
	//     db/name -> db_name_dirty
	//     db_name -> db_name_clean
	if database == "" {
		return databaseRootResource
	}

	db := strings.Replace(database, "/", "_", -1)
	if db == database {
		db += cleanSuffix
	} else {
		db += dirtySuffix
	}
	return path.Join(databaseRootResource, db)
}
