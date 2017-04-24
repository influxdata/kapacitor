// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
// +build !go1.6

package ber

const (
	tagBoolean         = 1
	tagInteger         = 2
	tagBitString       = 3
	tagOctetString     = 4
	tagOID             = 6
	tagEnum            = 10
	tagUTF8String      = 12
	tagSequence        = 16
	tagSet             = 17
	tagPrintableString = 19
	tagT61String       = 20
	tagIA5String       = 22
	tagUTCTime         = 23
	tagGeneralizedTime = 24
	tagGeneralString   = 27
)

const (
	classUniversal       = 0
	classApplication     = 1
	classContextSpecific = 2
	classPrivate         = 3
)
