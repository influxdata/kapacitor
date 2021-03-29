package auth

import (
	"bytes"
	"encoding/gob"
	"errors"

	"github.com/influxdata/kapacitor/services/storage"
)

var (
	ErrUserExists   = errors.New("user already exists")
	ErrNoUserExists = errors.New("no user exists")
)

type UserDAO interface {
	// Retrieve a user
	Get(username string) (User, error)

	// Create a user.
	// ErrUserExists is returned if a user already exists with the same username.
	Create(u User) error

	// Replace an existing user.
	// ErrNoUserExists is returned if the user does not exist.
	Replace(u User) error

	// Delete a user.
	// It is not an error to delete an non-existent user.
	Delete(username string) error

	// List users matching a pattern on username.
	// The pattern is shell/glob matching see https://golang.org/pkg/path/#Match
	// Offset and limit are pagination bounds. Offset is inclusive starting at index 0.
	// More results may exist while the number of returned items is equal to limit.
	List(pattern string, offset, limit int) ([]User, error)
}

//--------------------------------------------------------------------
// The following structures are stored in a database via gob encoding.
// Changes to the structures could break existing data.
//
// Many of these structures are exact copies of structures found elsewhere,
// this is intentional so that all structures stored in the database are
// defined here and nowhere else. So as to not accidentally change
// the gob serialization format in incompatible ways.

type Privilege int

const (
	NoPrivileges Privilege = iota

	ReadPrivilege
	WritePrivilege
	DeletePrivilege

	AllPrivileges
)

// A user with is set of permissions
type User struct {
	Name       string
	Admin      bool
	Hash       []byte
	Privileges map[string][]Privilege
}

type rawUser User

func (u User) ObjectID() string {
	return u.Name
}

func (u User) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(rawUser(u))
	return buf.Bytes(), err
}

func (u *User) UnmarshalBinary(data []byte) error {
	dec := gob.NewDecoder(bytes.NewReader(data))
	return dec.Decode((*rawUser)(u))
}

const (
	// Name of username index
	usernameIndex = "username"
)

// Key/Value store based implementation of the UserDAO
type userKV struct {
	store *storage.IndexedStore
}

func newUserKV(store storage.Interface) (*userKV, error) {
	c := storage.DefaultIndexedStoreConfig("users", func() storage.BinaryObject {
		return new(User)
	})
	c.Indexes = []storage.Index{{
		Name:   usernameIndex,
		Unique: true,
		ValueFunc: func(o storage.BinaryObject) (string, error) {
			return o.ObjectID(), nil
		},
	}}
	istore, err := storage.NewIndexedStore(store, c)
	if err != nil {
		return nil, err
	}
	return &userKV{
		store: istore,
	}, nil
}

func (kv *userKV) error(err error) error {
	if err == storage.ErrNoObjectExists {
		return ErrNoUserExists
	} else if err == storage.ErrObjectExists {
		return ErrUserExists
	}
	return err
}

func (kv *userKV) Get(username string) (User, error) {
	o, err := kv.store.Get(username)
	if err != nil {
		return User{}, kv.error(err)
	}
	u, ok := o.(*User)
	if !ok {
		return User{}, storage.ImpossibleTypeErr(u, o)
	}
	return *u, nil
}

func (kv *userKV) Create(u User) error {
	return kv.error(kv.store.Create(&u))
}

func (kv *userKV) Replace(u User) error {
	return kv.error(kv.store.Replace(&u))
}

func (kv *userKV) Delete(username string) error {
	return kv.error(kv.store.Delete(username))
}

func (kv *userKV) List(pattern string, offset, limit int) ([]User, error) {
	objects, err := kv.store.List(usernameIndex, pattern, offset, limit)
	if err != nil {
		return nil, kv.error(err)
	}
	users := make([]User, len(objects))
	for i, o := range objects {
		u, ok := o.(*User)
		if !ok {
			return nil, storage.ImpossibleTypeErr(u, o)
		}
		users[i] = *u
	}
	return users, nil
}
