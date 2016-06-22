package uuid

/***************
 * Date: 14/02/14
 * Time: 7:44 PM
 ***************/

import (
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
)

// ****************************************************

const (
	length = 16

	// 3f used by RFC4122 although 1f works for all
	variantSet = 0x3f

	// rather than using 0xc0 we use 0xe0 to retrieve the variant
	// The result is the same for all other variants
	// 0x80 and 0xa0 are used to identify RFC4122 compliance
	variantGet = 0xe0
)

var (
	// The following standard UUIDs are for use with V3 or V5 UUIDs.
	NamespaceDNS  UUID = namespaceUuid(0x6ba7b810)
	NamespaceURL  UUID = namespaceUuid(0x6ba7b811)
	NamespaceOID  UUID = namespaceUuid(0x6ba7b812)
	NamespaceX500 UUID = namespaceUuid(0x6ba7b814)

	generator *Generator
)

// ****************************************************

func init() {
	registerDefaultGenerator()
}

func namespaceUuid(pID uint32) UUID {
	// nodeID is the default Namespace node
	// 00.192.79.212.48.200
	return &uuid{pID, 0x9dad, 0x11d1, 0x80, 0xb4, uint8(length),
		[]byte{0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8},
	}
}

func registerDefaultGenerator() {
	generator = newGenerator(
		(&spinner{
			Resolution: defaultSpinResolution,
			Timestamp:  Now(),
			Count:      0,
		}).next,
		findFirstHardwareAddress,
		CleanHyphen)
}

// Generate a new RFC4122 version 1 UUID
// based on a 60 bit timestamp and node id
func NewV1() UUID {
	return generator.NewV1()
}

// Generate a new DCE Security version UUID
// based on a 60 bit timestamp, node id and POSIX UID or GUID
func NewV2(pDomain DCEDomain) UUID {
	return generator.NewV2(pDomain)
}

// Generates a new RFC4122 version 3 UUID
// Based on the MD5 hash of a namespace UUID and
// any type which implements the UniqueName interface for the name.
// For strings and slices cast to a Name type
func NewV3(pNs UUID, pName UniqueName) UUID {
	o := new(array)
	// Set all bits to MD5 hash generated from namespace and name.
	digest(o, pNs, pName, md5.New())
	o.setRFC4122Variant()
	o.setVersion(3)
	return o
}

// Generates a new RFC4122 version 4 UUID
// A cryptographically secure random UUID.
func NewV4() UUID {
	o := new(array)
	// Read random values (or pseudo-random) into array type.
	rand.Read(o[:length])
	o.setRFC4122Variant()
	o.setVersion(4)
	return o
}

// Generates a new RFC4122 version 5 UUID
// based on the SHA-1 hash of a namespace
// UUID and a unique name.
func NewV5(pNs UUID, pName UniqueName) UUID {
	o := new(array)
	digest(o, pNs, pName, sha1.New())
	o.setRFC4122Variant()
	o.setVersion(5)
	return o
}
