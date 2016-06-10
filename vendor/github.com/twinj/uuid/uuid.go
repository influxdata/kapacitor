// This package provides RFC4122 UUIDs.
//
// NewV1, NewV2, NewV3, NewV4, NewV5, for generating versions 1, 3, 4
// and 5 UUIDs as specified in RFC-4122.
//
// New([]byte), unsafe; NewHex(string); and Parse(string) for
// creating UUIDs from existing data.
//
// The original version was from Krzysztof Kowalik <chris@nu7hat.ch>
// Unfortunately, that version was non compliant with RFC4122.
// I have since heavily redesigned it.
//
// The example code in the specification was also used as reference
// for design.
//
// Copyright (C) 2014 twinj@github.com  2014 MIT style licence
package uuid

/****************
 * Date: 31/01/14
 * Time: 3:35 PM
 ***************/

import (
	"bytes"
	"encoding"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"regexp"
	"strings"
)

const (
	ReservedNCS       uint8 = 0x00
	ReservedRFC4122   uint8 = 0x80 // or and A0 if masked with 1F
	ReservedMicrosoft uint8 = 0xC0
	ReservedFuture    uint8 = 0xE0
)

type DCEDomain uint8

const (
	DomainPerson DCEDomain = iota
	DomainGroup
)

const (

	// Pattern used to parse string representation of the UUID.
	// Current one allows to parse string where only one opening
	// or closing bracket or any of the hyphens are optional.
	// It is only used to extract the main bytes to create a UUID,
	// so these imperfections are of no consequence.
	hexPattern = `^(urn\:uuid\:)?[\{\(\[]?([[:xdigit:]]{8})-?([[:xdigit:]]{4})-?([1-5][[:xdigit:]]{3})-?([[:xdigit:]]{4})-?([[:xdigit:]]{12})[\]\}\)]?$`
)

var (
	parseUUIDRegex = regexp.MustCompile(hexPattern)
)

type Node []byte
type Sequence uint16

// ******************************************************  UUID

// Interface for all UUIDs
type UUID interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler

	// Marshals the UUID bytes or data
	Bytes() (data []byte)

	// Organises data into a new UUID
	Unmarshal(pData []byte)

	// Size is used where different implementations require
	// different sizes. Should return the number of bytes in
	// the implementation.
	// Enables unmarshal and Bytes to screen for size
	Size() int

	// Version returns a version number of the algorithm used
	// to generate the UUID.
	// This may may behave independently across non RFC4122 UUIDs
	Version() int

	// Variant returns the UUID Variant
	// This will be one of the constants:
	// ReservedRFC4122,
	// ReservedMicrosoft,
	// ReservedFuture,
	// ReservedNCS.
	// This may behave differently across non RFC4122 UUIDs
	Variant() uint8

	// A UUID can be used as a Name within a namespace
	// Is simply just a String() string, method
	// Returns a formatted version of the UUID.
	UniqueName
}

// Creates a UUID from a slice of bytes.
// Truncates any bytes past the default length of 16
// Will panic if data slice is too small.
func New(pData []byte) UUID {
	o := new(array)
	o.Unmarshal(pData)
	return o
}

// Creates a UUID from a hex string
// Will panic if hex string is invalid - will panic even with hyphens and brackets
// Expects a clean string use Parse otherwise.
func NewHex(pUuid string) UUID {
	bytes, err := hex.DecodeString(pUuid)
	if err != nil {
		panic(err)
	}
	return New(bytes)
}

// Creates a UUID from a valid string representation.
// Accepts UUID string in following formats:
//		6ba7b8149dad11d180b400c04fd430c8
//		6ba7b814-9dad-11d1-80b4-00c04fd430c8
//		{6ba7b814-9dad-11d1-80b4-00c04fd430c8}
//		urn:uuid:6ba7b814-9dad-11d1-80b4-00c04fd430c8
//		[6ba7b814-9dad-11d1-80b4-00c04fd430c8]
//
func Parse(pUUID string) (UUID, error) {
	md := parseUUIDRegex.FindStringSubmatch(pUUID)
	if md == nil {
		return nil, errors.New("uuid.Parse: invalid string")
	}
	return NewHex(md[2] + md[3] + md[4] + md[5] + md[6]), nil
}

func digest(o, pNs UUID, pName UniqueName, pHash hash.Hash) {
	// Hash writer never returns an error
	pHash.Write(pNs.Bytes())
	pHash.Write([]byte(pName.String()))
	o.Unmarshal(pHash.Sum(nil)[:o.Size()])
}

// Function provides a safe way to unmarshal bytes into an
// existing UUID.
// Checks for length.
func UnmarshalBinary(o UUID, pData []byte) error {
	if len(pData) != o.Size() {
		return errors.New("uuid.UnmarshalBinary: invalid length")
	}
	o.Unmarshal(pData)
	return nil
}

// **********************************************  UUID Names

// A UUID Name is a simple string which implements UniqueName
// which satisfies the Stringer interface.
type Name string

// Returns the name as a string. Satisfies the Stringer interface.
func (o Name) String() string {
	return string(o)
}

// NewName will create a unique name from several sources
func NewName(pSalt string, pNames ...UniqueName) UniqueName {
	var s string
	for _, s2 := range pNames {
		s += s2.String()
	}
	return Name(s + pSalt)
}

// UniqueName is a Stinger interface
// Made for easy passing of IPs, URLs, the several Address types,
// Buffers and any other type which implements Stringer
// string, []byte types and Hash sums will need to be cast to
// the Name type or some other type which implements
// Stringer or UniqueName
type UniqueName interface {

	// Many go types implement this method for use with printing
	// Will convert the current type to its native string format
	String() string
}

// **********************************************  UUID Printing

// A Format is a pattern used by the stringer interface with which to print
// the UUID.
type Format string

const (
	Clean   Format = "%x%x%x%x%x%x"
	Curly   Format = "{%x%x%x%x%x%x}"
	Bracket Format = "(%x%x%x%x%x%x)"

	// This is the default format.
	CleanHyphen Format = "%x-%x-%x-%x%x-%x"

	CurlyHyphen   Format = "{%x-%x-%x-%x%x-%x}"
	BracketHyphen Format = "(%x-%x-%x-%x%x-%x)"
	GoIdFormat    Format = "[%X-%X-%x-%X%X-%x]"
)

func newGenerator(fNext func() Timestamp, fId func() Node, pFmt Format) (generator *Generator) {
	generator = new(Generator)
	generator.Fmt = string(pFmt)
	generator.Next = fNext
	generator.Id = fId
	return
}

// Switches the default printing format for ALL UUID strings
// A valid format will have 6 groups if the supplied Format does not
func SwitchFormat(pFormat Format) {
	if strings.Count(string(pFormat), "%") != 6 {
		panic(errors.New("uuid.switchFormat: invalid formatting"))
	}
	generator.Fmt = string(pFormat)
}

// Same as SwitchFormat but will make it uppercase
func SwitchFormatUpperCase(pFormat Format) {
	form := strings.ToUpper(string(pFormat))
	SwitchFormat(Format(form))
}

// Compares whether each UUID is the same
func Equal(p1 UUID, p2 UUID) bool {
	return bytes.Equal(p1.Bytes(), p2.Bytes())
}

// Print a UUID into a human readable string which matches the given Format
// Use this for one time formatting when setting the default using SwitchFormat
// The format must cater for each 6 UUID value groups
func Sprintf(pFormat Format, pId UUID) string {
	fmt := string(pFormat)
	if strings.Count(fmt, "%") != 6 {
		panic(errors.New("uuid.Print: invalid format"))
	}
	return formatter(pId, fmt)
}

// ***************************************************  Helpers

func variant(pVariant uint8) uint8 {
	switch pVariant & variantGet {
	case ReservedRFC4122, 0xA0:
		return ReservedRFC4122
	case ReservedMicrosoft:
		return ReservedMicrosoft
	case ReservedFuture:
		return ReservedFuture
	}
	return ReservedNCS
}

func setVariant(pByte *byte, pVariant uint8) {
	switch pVariant {
	case ReservedRFC4122:
		*pByte &= variantSet
	case ReservedFuture, ReservedMicrosoft:
		*pByte &= 0x1F
	case ReservedNCS:
		*pByte &= 0x7F
	default:
		panic(errors.New("uuid.setVariant: invalid variant mask"))
	}
	*pByte |= pVariant
}

func formatter(pId UUID, pFormat string) string {
	b := pId.Bytes()
	return fmt.Sprintf(pFormat, b[0:4], b[4:6], b[6:8], b[8:9], b[9:10], b[10:pId.Size()])
}
