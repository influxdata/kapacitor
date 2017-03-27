// Package uuid generates and parses UUIDs.
// A simple API is exposed while the implemenation is provided by github.com/google/uuid
package uuid

import "github.com/google/uuid"

// UUID is a 16 byte (128 bit) id, and can be used as a map key and in direct comparisons.
type UUID uuid.UUID

// Nil represents an invalid or empty UUID.
var Nil = UUID(uuid.Nil)

func New() UUID {
	return UUID(uuid.New())
}

// Must returns u or panics if err is not nil.
func Must(u UUID, err error) UUID {
	if err != nil {
		panic(err)
	}
	return u
}

// Parse an UUID of the forms "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" and
// "urn:uuid:xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx".
func Parse(s string) (UUID, error) {
	u, err := uuid.Parse(s)
	return UUID(u), err
}

// Parse an UUID of the forms "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" and
// "urn:uuid:xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" represented as a byte slice.
func ParseBytes(b []byte) (UUID, error) {
	u, err := uuid.ParseBytes(b)
	return UUID(u), err
}

// String represents the UUID in the form "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx".
func (u UUID) String() string {
	return uuid.UUID(u).String()
}

func (u UUID) MarshalBinary() ([]byte, error) {
	return uuid.UUID(u).MarshalBinary()
}
func (u *UUID) UnmarshalBinary(data []byte) error {
	return (*uuid.UUID)(u).UnmarshalBinary(data)
}

func (u UUID) MarshalText() ([]byte, error) {
	return uuid.UUID(u).MarshalText()
}
func (u *UUID) UnmarshalText(data []byte) error {
	return (*uuid.UUID)(u).UnmarshalText(data)
}
