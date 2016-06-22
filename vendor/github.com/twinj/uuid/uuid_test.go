package uuid

/****************
 * Date: 3/02/14
 * Time: 10:59 PM
 ***************/

import (
	"crypto/md5"
	"crypto/sha1"
	"fmt"
	"github.com/stretchr/testify/assert"
	"regexp"
	"testing"
)

var (
	uuid_goLang Name = "https://google.com/golang.org?q=golang"

	printer bool = false

	uuidBytes = [length]byte{
		0xaa, 0xcf, 0xee, 0x12,
		0xd4, 0x00,
		0x27, 0x23,
		0x00,
		0xd3,
		0x23, 0x12, 0x4a, 0x11, 0x89, 0xbb,
	}

	idString = "aacfee12-d400-2723-00d3-23124a1189bb"

	uuidVariants = []byte{
		ReservedNCS, ReservedRFC4122, ReservedMicrosoft, ReservedFuture,
	}
	namespaceUuids = []UUID{
		NamespaceDNS, NamespaceURL, NamespaceOID, NamespaceX500,
	}

	invalidHexStrings = [...]string{
		"foo",
		"6ba7b814-9dad-11d1-80b4-",
		"6ba7b814--9dad-11d1-80b4--00c04fd430c8",
		"6ba7b814-9dad7-11d1-80b4-00c04fd430c8999",
		"{6ba7b814-9dad-1180b4-00c04fd430c8",
		"{6ba7b814--11d1-80b4-00c04fd430c8}",
		"urn:uuid:6ba7b814-9dad-1666666680b4-00c04fd430c8",
	}

	validHexStrings = [...]string{
		"6ba7b8149dad-11d1-80b4-00c04fd430c8}",
		"{6ba7b8149dad-11d1-80b400c04fd430c8}",
		"{6ba7b814-9dad11d180b400c04fd430c8}",
		"6ba7b8149dad-11d1-80b4-00c04fd430c8",
		"6ba7b814-9dad11d1-80b4-00c04fd430c8",
		"6ba7b814-9dad-11d180b4-00c04fd430c8",
		"6ba7b814-9dad-11d1-80b400c04fd430c8",
		"6ba7b8149dad11d180b400c04fd430c8",
		"6ba7b814-9dad-11d1-80b4-00c04fd430c8",
		"{6ba7b814-9dad-11d1-80b4-00c04fd430c8}",
		"{6ba7b814-9dad-11d1-80b4-00c04fd430c8",
		"6ba7b814-9dad-11d1-80b4-00c04fd430c8}",
		"(6ba7b814-9dad-11d1-80b4-00c04fd430c8)",
		"urn:uuid:6ba7b814-9dad-11d1-80b4-00c04fd430c8",
	}
)

func TestEqual(t *testing.T) {
	for k, v := range namespaces {
		u, _ := Parse(v)
		assert.True(t, Equal(k, u), "Id's should be equal")
		assert.Equal(t, k.String(), u.String(), "Stringer versions should equal")
	}
}

func TestNewHex(t *testing.T) {
	s := "f3593cffee9240df408687825b523f13"
	u := NewHex(s)
	assert.Equal(t, 4, u.Version(), "Expected correct version")
	assert.Equal(t, ReservedNCS, u.Variant(), "Expected correct variant")
	assert.True(t, parseUUIDRegex.MatchString(u.String()), "Expected string representation to be valid")

	assert.True(t, didNewHexPanic(), "Hex string should panic when invalid")
}

func didNewHexPanic() bool {
	return func() (didPanic bool) {
		defer func() {
			if recover() != nil {
				didPanic = true
			}
		}()

		NewHex("*********-------)()()()()(")
		return
	}()
}

func TestParse(t *testing.T) {
	for _, v := range invalidHexStrings {
		_, err := Parse(v)
		assert.Error(t, err, "Expected error due to invalid UUID string")
	}
	for _, v := range validHexStrings {
		_, err := Parse(v)
		assert.NoError(t, err, "Expected valid UUID string but got error")
	}
	for _, id := range namespaceUuids {
		_, err := Parse(id.String())
		assert.NoError(t, err, "Expected valid UUID string but got error")
	}
}

func TestNew(t *testing.T) {
	for k, _ := range namespaces {

		u := New(k.Bytes())

		assert.NotNil(t, u, "Expected a valid non nil UUID")
		assert.Equal(t, 1, u.Version(), "Expected correct version %d, but got %d", 2, u.Version())
		assert.Equal(t, ReservedRFC4122, u.Variant(), "Expected ReservedNCS variant %x, but got %x", ReservedNCS, u.Variant())
		assert.Equal(t, k.String(), u.String(), "Stringer versions should equal")
	}
}

func TestUUID_NewBulk(t *testing.T) {
	for i := 0; i < 1000000; i++ {
		New(uuidBytes[:])
	}
}

const (
	clean                   = `[[:xdigit:]]{8}[[:xdigit:]]{4}[1-5][[:xdigit:]]{3}[[:xdigit:]]{4}[[:xdigit:]]{12}`
	cleanHexPattern         = `^` + clean + `$`
	curlyHexPattern         = `^\{` + clean + `\}$`
	bracketHexPattern       = `^\(` + clean + `\)$`
	hyphen                  = `[[:xdigit:]]{8}-[[:xdigit:]]{4}-[1-5][[:xdigit:]]{3}-[[:xdigit:]]{4}-[[:xdigit:]]{12}`
	cleanHyphenHexPattern   = `^` + hyphen + `$`
	curlyHyphenHexPattern   = `^\{` + hyphen + `\}$`
	bracketHyphenHexPattern = `^\(` + hyphen + `\)$`
)

func TestFormat(t *testing.T) {
	ids := []UUID{NewV4(), NewV1()}
	formats := []Format{CurlyHyphen, Clean, Curly, Bracket, CleanHyphen, BracketHyphen, GoIdFormat}
	patterns := []string{curlyHyphenHexPattern, cleanHexPattern, curlyHexPattern, bracketHexPattern, cleanHyphenHexPattern, bracketHyphenHexPattern, hyphen}

	// Reset default
	SwitchFormat(CleanHyphen)

	for _, u := range ids {
		for i := range formats {
			SwitchFormatUpperCase(formats[i])
			assert.True(t, regexp.MustCompile(patterns[i]).MatchString(u.String()), "Uppercase format %s must compile pattern %s", formats[i], patterns[i])

			SwitchFormat(formats[i])
			assert.True(t, regexp.MustCompile(patterns[i]).MatchString(u.String()), "Format %s must compile pattern %s", formats[i], patterns[i])
			outputLn(u)
		}
	}

	// Reset default
	SwitchFormat(CleanHyphen)
}

func TestSwitchFormat(t *testing.T) {
	ids := []UUID{NewV4(), NewV1()}
	formats := []Format{CurlyHyphen, Clean, Curly, Bracket, CleanHyphen, BracketHyphen, GoIdFormat}
	patterns := []string{curlyHyphenHexPattern, cleanHexPattern, curlyHexPattern, bracketHexPattern, cleanHyphenHexPattern, bracketHyphenHexPattern, hyphen}

	// Reset default
	SwitchFormat(CleanHyphen)

	for _, u := range ids {
		for i := range formats {
			SwitchFormatUpperCase(formats[i])
			assert.True(t, regexp.MustCompile(patterns[i]).MatchString(u.String()), "Uppercase format %s must compile pattern %s", formats[i], patterns[i])

			SwitchFormat(formats[i])
			assert.True(t, regexp.MustCompile(patterns[i]).MatchString(u.String()), "Format %s must compile pattern %s", formats[i], patterns[i])
			outputLn(u)
		}
	}

	assert.True(t, didSwitchFormatPanic(), "Switch format should panic when format invalid")

	// Reset default
	SwitchFormat(CleanHyphen)
}

func didSwitchFormatPanic() bool {
	return func() (didPanic bool) {
		defer func() {
			if recover() != nil {
				didPanic = true
			}
		}()

		SwitchFormat("%%%%%%%%%%%%%")
		return
	}()
}

func TestSwitchFormatUpperCase(t *testing.T) {
	ids := []UUID{NewV4(), NewV1()}
	formats := []Format{CurlyHyphen, Clean, Curly, Bracket, CleanHyphen, BracketHyphen, GoIdFormat}
	patterns := []string{curlyHyphenHexPattern, cleanHexPattern, curlyHexPattern, bracketHexPattern, cleanHyphenHexPattern, bracketHyphenHexPattern, hyphen}

	// Reset default
	SwitchFormat(CleanHyphen)

	for _, u := range ids {
		for i := range formats {
			SwitchFormatUpperCase(formats[i])
			assert.True(t, regexp.MustCompile(patterns[i]).MatchString(u.String()), "Uppercase format %s must compile pattern %s", formats[i], patterns[i])
		}
	}

	// Reset default
	SwitchFormat(CleanHyphen)
}

func TestSprintf(t *testing.T) {
	ids := []UUID{NewV4(), NewV1()}
	formats := []Format{CurlyHyphen, Clean, Curly, Bracket, CleanHyphen, BracketHyphen, GoIdFormat}
	patterns := []string{curlyHyphenHexPattern, cleanHexPattern, curlyHexPattern, bracketHexPattern, cleanHyphenHexPattern, bracketHyphenHexPattern, hyphen}

	for _, u := range ids {
		for i := range formats {
			assert.True(t, regexp.MustCompile(patterns[i]).MatchString(Sprintf(formats[i], u)), "Format must compile")
			outputLn(Sprintf(formats[i], u))
		}
	}

	assert.True(t, didSprintfPanic(), "Sprinf should panic when format invalid")
}

func didSprintfPanic() bool {
	return func() (didPanic bool) {
		defer func() {
			if recover() != nil {
				didPanic = true
			}
		}()

		Sprintf("*********-------)()()()()(", NamespaceDNS)
		return
	}()
}

func TestUUID_NewHexBulk(t *testing.T) {
	for i := 0; i < 1000000; i++ {
		s := "f3593cffee9240df408687825b523f13"
		NewHex(s)
	}
}

func TestUUID_Sum(t *testing.T) {
	u := new(array)
	digest(u, NamespaceDNS, uuid_goLang, md5.New())
	if u.Bytes() == nil {
		t.Error("Expected new data in bytes")
	}
	output(u.Bytes())
	u = new(array)
	digest(u, NamespaceDNS, uuid_goLang, sha1.New())
	if u.Bytes() == nil {
		t.Error("Expected new data in bytes")
	}
	output(u.Bytes())
}

// *******************************************************

func tVariantConstraint(v byte, b byte, o UUID, t *testing.T) {
	output(o)
	switch v {
	case ReservedNCS:
		switch b {
		case 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07:
			outputF(": %X ", b)
			break
		default:
			t.Errorf("%X most high bits do not resolve to 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07", b)
		}
	case ReservedRFC4122:
		switch b {
		case 0x08, 0x09, 0x0A, 0x0B:
			outputF(": %X ", b)
			break
		default:
			t.Errorf("%X most high bits do not resolve to 0x08, 0x09, 0x0A, 0x0B", b)
		}
	case ReservedMicrosoft:
		switch b {
		case 0x0C, 0x0D:
			outputF(": %X ", b)
			break
		default:
			t.Errorf("%X most high bits do not resolve to 0x0C, 0x0D", b)
		}
	case ReservedFuture:
		switch b {
		case 0x0E, 0x0F:
			outputF(": %X ", b)
			break
		default:
			t.Errorf("%X most high bits do not resolve to 0x0E, 0x0F", b)
		}
	}
	output("\n")
}

func output(a ...interface{}) {
	if printer {
		fmt.Print(a...)
	}
}

func outputLn(a ...interface{}) {
	if printer {
		fmt.Println(a...)
	}
}

func outputF(format string, a ...interface{}) {
	if printer {
		fmt.Printf(format, a)
	}
}
