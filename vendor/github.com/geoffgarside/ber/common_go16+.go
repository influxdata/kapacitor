// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
// +build go1.6

package ber

import "encoding/asn1"

const (
	tagBoolean         = asn1.TagBoolean
	tagInteger         = asn1.TagInteger
	tagBitString       = asn1.TagBitString
	tagOctetString     = asn1.TagOctetString
	tagOID             = asn1.TagOID
	tagEnum            = asn1.TagEnum
	tagUTF8String      = asn1.TagUTF8String
	tagSequence        = asn1.TagSequence
	tagSet             = asn1.TagSet
	tagPrintableString = asn1.TagPrintableString
	tagT61String       = asn1.TagT61String
	tagIA5String       = asn1.TagIA5String
	tagUTCTime         = asn1.TagUTCTime
	tagGeneralizedTime = asn1.TagGeneralizedTime
	tagGeneralString   = asn1.TagGeneralString
)

const (
	classUniversal       = asn1.ClassUniversal
	classApplication     = asn1.ClassApplication
	classContextSpecific = asn1.ClassContextSpecific
	classPrivate         = asn1.ClassPrivate
)
