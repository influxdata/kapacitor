// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package ber

import (
	"bytes"
	"encoding/asn1"
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"testing"
	"time"
)

type boolTest struct {
	in  []byte
	ok  bool
	out bool
}

var boolTestData = []boolTest{
	{[]byte{0x00}, true, false},
	{[]byte{0xff}, true, true},
	{[]byte{0x00, 0x00}, false, false},
	{[]byte{0xff, 0xff}, false, false},
	{[]byte{0x01}, false, false},
}

func TestParseBool(t *testing.T) {
	for i, test := range boolTestData {
		ret, err := parseBool(test.in)
		if (err == nil) != test.ok {
			t.Errorf("#%d: Incorrect error result (did fail? %v, expected: %v)", i, err == nil, test.ok)
		}
		if test.ok && ret != test.out {
			t.Errorf("#%d: Bad result: %v (expected %v)", i, ret, test.out)
		}
	}
}

type int64Test struct {
	in  []byte
	ok  bool
	out int64
}

var int64TestData = []int64Test{
	{[]byte{0x00}, true, 0},
	{[]byte{0x7f}, true, 127},
	{[]byte{0x00, 0x80}, true, 128},
	{[]byte{0x01, 0x00}, true, 256},
	{[]byte{0x80}, true, -128},
	{[]byte{0xff, 0x7f}, true, -129},
	{[]byte{0xff}, true, -1},
	{[]byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, true, -9223372036854775808},
	{[]byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, false, 0},
	{[]byte{}, false, 0},
	{[]byte{0x00, 0x7f}, false, 0},
	{[]byte{0xff, 0xf0}, false, 0},
}

func TestParseInt64(t *testing.T) {
	for i, test := range int64TestData {
		ret, err := parseInt64(test.in)
		if (err == nil) != test.ok {
			t.Errorf("#%d: Incorrect error result (did fail? %v, expected: %v)", i, err == nil, test.ok)
		}
		if test.ok && ret != test.out {
			t.Errorf("#%d: Bad result: %v (expected %v)", i, ret, test.out)
		}
	}
}

type int32Test struct {
	in  []byte
	ok  bool
	out int32
}

var int32TestData = []int32Test{
	{[]byte{0x00}, true, 0},
	{[]byte{0x7f}, true, 127},
	{[]byte{0x00, 0x80}, true, 128},
	{[]byte{0x01, 0x00}, true, 256},
	{[]byte{0x80}, true, -128},
	{[]byte{0xff, 0x7f}, true, -129},
	{[]byte{0xff}, true, -1},
	{[]byte{0x80, 0x00, 0x00, 0x00}, true, -2147483648},
	{[]byte{0x80, 0x00, 0x00, 0x00, 0x00}, false, 0},
	{[]byte{}, false, 0},
	{[]byte{0x00, 0x7f}, false, 0},
	{[]byte{0xff, 0xf0}, false, 0},
}

func TestParseInt32(t *testing.T) {
	for i, test := range int32TestData {
		ret, err := parseInt32(test.in)
		if (err == nil) != test.ok {
			t.Errorf("#%d: Incorrect error result (did fail? %v, expected: %v)", i, err == nil, test.ok)
		}
		if test.ok && int32(ret) != test.out {
			t.Errorf("#%d: Bad result: %v (expected %v)", i, ret, test.out)
		}
	}
}

var bigIntTests = []struct {
	in     []byte
	ok     bool
	base10 string
}{
	{[]byte{0xff}, true, "-1"},
	{[]byte{0x00}, true, "0"},
	{[]byte{0x01}, true, "1"},
	{[]byte{0x00, 0xff}, true, "255"},
	{[]byte{0xff, 0x00}, true, "-256"},
	{[]byte{0x01, 0x00}, true, "256"},
	{[]byte{}, false, ""},
	{[]byte{0x00, 0x7f}, false, ""},
	{[]byte{0xff, 0xf0}, false, ""},
}

func TestParseBigInt(t *testing.T) {
	for i, test := range bigIntTests {
		ret, err := parseBigInt(test.in)
		if (err == nil) != test.ok {
			t.Errorf("#%d: Incorrect error result (did fail? %v, expected: %v)", i, err == nil, test.ok)
		}
		if test.ok {
			if ret.String() != test.base10 {
				t.Errorf("#%d: bad result from %x, got %s want %s", i, test.in, ret.String(), test.base10)
			}
		}
	}
}

type bitStringTest struct {
	in        []byte
	ok        bool
	out       []byte
	bitLength int
}

var bitStringTestData = []bitStringTest{
	{[]byte{}, false, []byte{}, 0},
	{[]byte{0x00}, true, []byte{}, 0},
	{[]byte{0x07, 0x00}, true, []byte{0x00}, 1},
	{[]byte{0x07, 0x01}, false, []byte{}, 0},
	{[]byte{0x07, 0x40}, false, []byte{}, 0},
	{[]byte{0x08, 0x00}, false, []byte{}, 0},
}

func TestBitString(t *testing.T) {
	for i, test := range bitStringTestData {
		ret, err := parseBitString(test.in)
		if (err == nil) != test.ok {
			t.Errorf("#%d: Incorrect error result (did fail? %v, expected: %v)", i, err == nil, test.ok)
		}
		if err == nil {
			if test.bitLength != ret.BitLength || !bytes.Equal(ret.Bytes, test.out) {
				t.Errorf("#%d: Bad result: %v (expected %v %v)", i, ret, test.out, test.bitLength)
			}
		}
	}
}

type objectIdentifierTest struct {
	in  []byte
	ok  bool
	out []int
}

var objectIdentifierTestData = []objectIdentifierTest{
	{[]byte{}, false, []int{}},
	{[]byte{85}, true, []int{2, 5}},
	{[]byte{85, 0x02}, true, []int{2, 5, 2}},
	{[]byte{85, 0x02, 0xc0, 0x00}, true, []int{2, 5, 2, 0x2000}},
	{[]byte{0x81, 0x34, 0x03}, true, []int{2, 100, 3}},
	{[]byte{85, 0x02, 0xc0, 0x80, 0x80, 0x80, 0x80}, false, []int{}},

	// large object identifier value, seen on some SNMP devices
	// See ber_64bit_test.go for none 32-bit compatible examples.
	{
		[]byte{
			0x2b, 0x06, 0x01, 0x02, 0x01, 0x1f, 0x01, 0x01,
			0x01, 0x01, 0x84, 0x88, 0x90, 0x80, 0x23},
		true,
		[]int{1, 3, 6, 1, 2, 1, 31, 1, 1, 1, 1, 1090781219},
	},
}

func TestObjectIdentifier(t *testing.T) {
	for i, test := range objectIdentifierTestData {
		ret, err := parseObjectIdentifier(test.in)
		if (err == nil) != test.ok {
			t.Errorf("#%d: Incorrect error result (did fail? %v, expected: %v)", i, err == nil, test.ok)
		}
		if err == nil {
			if !reflect.DeepEqual(test.out, ret) {
				t.Errorf("#%d: Bad result: %v (expected %v)", i, ret, test.out)
			}
		}
	}

	if s := asn1.ObjectIdentifier([]int{1, 2, 3, 4}).String(); s != "1.2.3.4" {
		t.Errorf("bad ObjectIdentifier.String(). Got %s, want 1.2.3.4", s)
	}
}

type timeTest struct {
	in  string
	ok  bool
	out time.Time
}

var utcTestData = []timeTest{
	{"910506164540-0700", true, time.Date(1991, 05, 06, 16, 45, 40, 0, time.FixedZone("", -7*60*60))},
	{"910506164540+0730", true, time.Date(1991, 05, 06, 16, 45, 40, 0, time.FixedZone("", 7*60*60+30*60))},
	{"910506234540Z", true, time.Date(1991, 05, 06, 23, 45, 40, 0, time.UTC)},
	{"9105062345Z", true, time.Date(1991, 05, 06, 23, 45, 0, 0, time.UTC)},
	{"5105062345Z", true, time.Date(1951, 05, 06, 23, 45, 0, 0, time.UTC)},
	{"a10506234540Z", false, time.Time{}},
	{"91a506234540Z", false, time.Time{}},
	{"9105a6234540Z", false, time.Time{}},
	{"910506a34540Z", false, time.Time{}},
	{"910506334a40Z", false, time.Time{}},
	{"91050633444aZ", false, time.Time{}},
	{"910506334461Z", false, time.Time{}},
	{"910506334400Za", false, time.Time{}},
	/* These are invalid times. However, the time package normalises times
	 * and they were accepted in some versions. See #11134. */
	{"000100000000Z", false, time.Time{}},
	{"101302030405Z", false, time.Time{}},
	{"100002030405Z", false, time.Time{}},
	{"100100030405Z", false, time.Time{}},
	{"100132030405Z", false, time.Time{}},
	{"100231030405Z", false, time.Time{}},
	{"100102240405Z", false, time.Time{}},
	{"100102036005Z", false, time.Time{}},
	{"100102030460Z", false, time.Time{}},
	{"-100102030410Z", false, time.Time{}},
	{"10-0102030410Z", false, time.Time{}},
	{"10-0002030410Z", false, time.Time{}},
	{"1001-02030410Z", false, time.Time{}},
	{"100102-030410Z", false, time.Time{}},
	{"10010203-0410Z", false, time.Time{}},
	{"1001020304-10Z", false, time.Time{}},
}

func TestUTCTime(t *testing.T) {
	for i, test := range utcTestData {
		ret, err := parseUTCTime([]byte(test.in))
		if err != nil {
			if test.ok {
				t.Errorf("#%d: parseUTCTime(%q) = error %v", i, test.in, err)
			}
			continue
		}
		if !test.ok {
			t.Errorf("#%d: parseUTCTime(%q) succeeded, should have failed", i, test.in)
			continue
		}
		const format = "Jan _2 15:04:05 -0700 2006" // ignore zone name, just offset
		have := ret.Format(format)
		want := test.out.Format(format)
		if have != want {
			t.Errorf("#%d: parseUTCTime(%q) = %s, want %s", i, test.in, have, want)
		}
	}
}

var generalizedTimeTestData = []timeTest{
	{"20100102030405Z", true, time.Date(2010, 01, 02, 03, 04, 05, 0, time.UTC)},
	{"20100102030405", false, time.Time{}},
	{"20100102030405+0607", true, time.Date(2010, 01, 02, 03, 04, 05, 0, time.FixedZone("", 6*60*60+7*60))},
	{"20100102030405-0607", true, time.Date(2010, 01, 02, 03, 04, 05, 0, time.FixedZone("", -6*60*60-7*60))},
	/* These are invalid times. However, the time package normalises times
	 * and they were accepted in some versions. See #11134. */
	{"00000100000000Z", false, time.Time{}},
	{"20101302030405Z", false, time.Time{}},
	{"20100002030405Z", false, time.Time{}},
	{"20100100030405Z", false, time.Time{}},
	{"20100132030405Z", false, time.Time{}},
	{"20100231030405Z", false, time.Time{}},
	{"20100102240405Z", false, time.Time{}},
	{"20100102036005Z", false, time.Time{}},
	{"20100102030460Z", false, time.Time{}},
	{"-20100102030410Z", false, time.Time{}},
	{"2010-0102030410Z", false, time.Time{}},
	{"2010-0002030410Z", false, time.Time{}},
	{"201001-02030410Z", false, time.Time{}},
	{"20100102-030410Z", false, time.Time{}},
	{"2010010203-0410Z", false, time.Time{}},
	{"201001020304-10Z", false, time.Time{}},
}

func TestGeneralizedTime(t *testing.T) {
	for i, test := range generalizedTimeTestData {
		ret, err := parseGeneralizedTime([]byte(test.in))
		if (err == nil) != test.ok {
			t.Errorf("#%d: Incorrect error result (did fail? %v, expected: %v)", i, err == nil, test.ok)
		}
		if err == nil {
			if !reflect.DeepEqual(test.out, ret) {
				t.Errorf("#%d: Bad result: %q → %v (expected %v)", i, test.in, ret, test.out)
			}
		}
	}
}

type tagAndLengthTest struct {
	in  []byte
	ok  bool
	out tagAndLength
}

var tagAndLengthData = []tagAndLengthTest{
	{[]byte{0x80, 0x01}, true, tagAndLength{2, 0, 1, false}},
	{[]byte{0xa0, 0x01}, true, tagAndLength{2, 0, 1, true}},
	{[]byte{0x02, 0x00}, true, tagAndLength{0, 2, 0, false}},
	{[]byte{0xfe, 0x00}, true, tagAndLength{3, 30, 0, true}},
	{[]byte{0x1f, 0x1f, 0x00}, true, tagAndLength{0, 31, 0, false}},
	{[]byte{0x1f, 0x81, 0x00, 0x00}, true, tagAndLength{0, 128, 0, false}},
	{[]byte{0x1f, 0x81, 0x80, 0x01, 0x00}, true, tagAndLength{0, 0x4001, 0, false}},
	{[]byte{0x00, 0x81, 0x80}, true, tagAndLength{0, 0, 128, false}},
	{[]byte{0x00, 0x82, 0x01, 0x00}, true, tagAndLength{0, 0, 256, false}},
	{[]byte{0x00, 0x83, 0x01, 0x00}, false, tagAndLength{}},
	{[]byte{0x1f, 0x85}, false, tagAndLength{}},
	{[]byte{0x30, 0x80}, false, tagAndLength{}},
	// Superfluous zeros in the length should be a accepted (different from DER).
	{[]byte{0xa0, 0x82, 0x00, 0xff}, true, tagAndLength{2, 0, 0xff, true}},
	// Lengths up to the maximum size of an int should work.
	{[]byte{0xa0, 0x84, 0x7f, 0xff, 0xff, 0xff}, true, tagAndLength{2, 0, 0x7fffffff, true}},
	// Lengths that would overflow an int should be rejected.
	{[]byte{0xa0, 0x84, 0x80, 0x00, 0x00, 0x00}, false, tagAndLength{}},
	{[]byte{0xa0, 0x84, 0x88, 0x90, 0x80, 0x23}, false, tagAndLength{}},
	// Long length form may be used for lengths that fit in short form (different from DER).
	{[]byte{0xa0, 0x81, 0x7f}, true, tagAndLength{2, 0, 0x7f, true}},
	// Tag numbers which would overflow int32 are rejected. (The value below is 2^31.)
	{[]byte{0x1f, 0x88, 0x80, 0x80, 0x80, 0x00, 0x00}, false, tagAndLength{}},
	// Long tag number form may not be used for tags that fit in short form.
	{[]byte{0x1f, 0x1e, 0x00}, false, tagAndLength{}},
}

func TestParseTagAndLength(t *testing.T) {
	for i, test := range tagAndLengthData {
		tagAndLength, _, err := parseTagAndLength(test.in, 0)
		if (err == nil) != test.ok {
			t.Errorf("#%d: Incorrect error result (did pass? %v, expected: %v)", i, err == nil, test.ok)
		}
		if err == nil && !reflect.DeepEqual(test.out, tagAndLength) {
			t.Errorf("#%d: Bad result: %v (expected %v)", i, tagAndLength, test.out)
		}
	}
}

type parseFieldParametersTest struct {
	in  string
	out fieldParameters
}

func newInt(n int) *int { return &n }

func newInt64(n int64) *int64 { return &n }

func newString(s string) *string { return &s }

func newBool(b bool) *bool { return &b }

var parseFieldParametersTestData []parseFieldParametersTest = []parseFieldParametersTest{
	{"", fieldParameters{}},
	{"ia5", fieldParameters{stringType: tagIA5String}},
	{"generalized", fieldParameters{timeType: tagGeneralizedTime}},
	{"utc", fieldParameters{timeType: tagUTCTime}},
	{"printable", fieldParameters{stringType: tagPrintableString}},
	{"optional", fieldParameters{optional: true}},
	{"explicit", fieldParameters{explicit: true, tag: new(int)}},
	{"application", fieldParameters{application: true, tag: new(int)}},
	{"optional,explicit", fieldParameters{optional: true, explicit: true, tag: new(int)}},
	{"default:42", fieldParameters{defaultValue: newInt64(42)}},
	{"tag:17", fieldParameters{tag: newInt(17)}},
	{"optional,explicit,default:42,tag:17", fieldParameters{optional: true, explicit: true, defaultValue: newInt64(42), tag: newInt(17)}},
	{"optional,explicit,default:42,tag:17,rubbish1", fieldParameters{true, true, false, newInt64(42), newInt(17), 0, 0, false, false}},
	{"set", fieldParameters{set: true}},
}

func TestParseFieldParameters(t *testing.T) {
	for i, test := range parseFieldParametersTestData {
		f := parseFieldParameters(test.in)
		if !reflect.DeepEqual(f, test.out) {
			t.Errorf("#%d: Bad result: %v (expected %v)", i, f, test.out)
		}
	}
}

type TestObjectIdentifierStruct struct {
	OID asn1.ObjectIdentifier
}

type TestContextSpecificTags struct {
	A int `asn1:"tag:1"`
}

type TestContextSpecificTags2 struct {
	A int `asn1:"explicit,tag:1"`
	B int
}

type TestContextSpecificTags3 struct {
	S string `asn1:"tag:1,utf8"`
}

type TestElementsAfterString struct {
	S    string
	A, B int
}

type TestBigInt struct {
	X *big.Int
}

type TestSet struct {
	Ints []int `asn1:"set"`
}

var unmarshalTestData = []struct {
	in  []byte
	out interface{}
}{
	{[]byte{0x02, 0x01, 0x42}, newInt(0x42)},
	{[]byte{0x30, 0x08, 0x06, 0x06, 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d}, &TestObjectIdentifierStruct{[]int{1, 2, 840, 113549}}},
	{[]byte{0x03, 0x04, 0x06, 0x6e, 0x5d, 0xc0}, &asn1.BitString{[]byte{110, 93, 192}, 18}},
	{[]byte{0x30, 0x09, 0x02, 0x01, 0x01, 0x02, 0x01, 0x02, 0x02, 0x01, 0x03}, &[]int{1, 2, 3}},
	{[]byte{0x02, 0x01, 0x10}, newInt(16)},
	{[]byte{0x13, 0x04, 't', 'e', 's', 't'}, newString("test")},
	{[]byte{0x16, 0x04, 't', 'e', 's', 't'}, newString("test")},
	{[]byte{0x16, 0x04, 't', 'e', 's', 't'}, &asn1.RawValue{0, 22, false, []byte("test"), []byte("\x16\x04test")}},
	{[]byte{0x04, 0x04, 1, 2, 3, 4}, &asn1.RawValue{0, 4, false, []byte{1, 2, 3, 4}, []byte{4, 4, 1, 2, 3, 4}}},
	{[]byte{0x30, 0x03, 0x81, 0x01, 0x01}, &TestContextSpecificTags{1}},
	{[]byte{0x30, 0x08, 0xa1, 0x03, 0x02, 0x01, 0x01, 0x02, 0x01, 0x02}, &TestContextSpecificTags2{1, 2}},
	{[]byte{0x30, 0x03, 0x81, 0x01, '@'}, &TestContextSpecificTags3{"@"}},
	{[]byte{0x01, 0x01, 0x00}, newBool(false)},
	{[]byte{0x01, 0x01, 0xff}, newBool(true)},
	{[]byte{0x30, 0x0b, 0x13, 0x03, 0x66, 0x6f, 0x6f, 0x02, 0x01, 0x22, 0x02, 0x01, 0x33}, &TestElementsAfterString{"foo", 0x22, 0x33}},
	{[]byte{0x30, 0x05, 0x02, 0x03, 0x12, 0x34, 0x56}, &TestBigInt{big.NewInt(0x123456)}},
	{[]byte{0x30, 0x0b, 0x31, 0x09, 0x02, 0x01, 0x01, 0x02, 0x01, 0x02, 0x02, 0x01, 0x03}, &TestSet{Ints: []int{1, 2, 3}}},
}

func TestUnmarshal(t *testing.T) {
	for i, test := range unmarshalTestData {
		pv := reflect.New(reflect.TypeOf(test.out).Elem())
		val := pv.Interface()
		_, err := Unmarshal(test.in, val)
		if err != nil {
			t.Errorf("Unmarshal failed at index %d %v", i, err)
		}
		if !reflect.DeepEqual(val, test.out) {
			t.Errorf("#%d:\nhave %#v\nwant %#v", i, val, test.out)
		}
	}
}

type AttributeTypeAndValue struct {
	Type  asn1.ObjectIdentifier
	Value interface{}
}

type rawStructTest struct {
	Raw asn1.RawContent
	A   int
}

func TestRawStructs(t *testing.T) {
	var s rawStructTest
	input := []byte{0x30, 0x03, 0x02, 0x01, 0x50}

	rest, err := Unmarshal(input, &s)
	if len(rest) != 0 {
		t.Errorf("incomplete parse: %x", rest)
		return
	}
	if err != nil {
		t.Error(err)
		return
	}
	if s.A != 0x50 {
		t.Errorf("bad value for A: got %d want %d", s.A, 0x50)
	}
	if !bytes.Equal([]byte(s.Raw), input) {
		t.Errorf("bad value for Raw: got %x want %x", s.Raw, input)
	}
}

var stringSliceTestData = [][]string{
	{"foo", "bar"},
	{"foo", "\\bar"},
	{"foo", "\"bar\""},
	{"foo", "åäö"},
}

func TestStringSlice(t *testing.T) {
	for _, test := range stringSliceTestData {
		bs, err := Marshal(test)
		if err != nil {
			t.Error(err)
		}

		var res []string
		_, err = Unmarshal(bs, &res)
		if err != nil {
			t.Error(err)
		}

		if fmt.Sprintf("%v", res) != fmt.Sprintf("%v", test) {
			t.Errorf("incorrect marshal/unmarshal; %v != %v", res, test)
		}
	}
}

type explicitTaggedTimeTest struct {
	Time time.Time `asn1:"explicit,tag:0"`
}

var explicitTaggedTimeTestData = []struct {
	in  []byte
	out explicitTaggedTimeTest
}{
	{[]byte{0x30, 0x11, 0xa0, 0xf, 0x17, 0xd, '9', '1', '0', '5', '0', '6', '1', '6', '4', '5', '4', '0', 'Z'},
		explicitTaggedTimeTest{time.Date(1991, 05, 06, 16, 45, 40, 0, time.UTC)}},
	{[]byte{0x30, 0x17, 0xa0, 0xf, 0x18, 0x13, '2', '0', '1', '0', '0', '1', '0', '2', '0', '3', '0', '4', '0', '5', '+', '0', '6', '0', '7'},
		explicitTaggedTimeTest{time.Date(2010, 01, 02, 03, 04, 05, 0, time.FixedZone("", 6*60*60+7*60))}},
}

func TestExplicitTaggedTime(t *testing.T) {
	// Test that a time.Time will match either tagUTCTime or
	// tagGeneralizedTime.
	for i, test := range explicitTaggedTimeTestData {
		var got explicitTaggedTimeTest
		_, err := Unmarshal(test.in, &got)
		if err != nil {
			t.Errorf("Unmarshal failed at index %d %v", i, err)
		}
		if !got.Time.Equal(test.out.Time) {
			t.Errorf("#%d: got %v, want %v", i, got.Time, test.out.Time)
		}
	}
}

type implicitTaggedTimeTest struct {
	Time time.Time `asn1:"tag:24"`
}

func TestImplicitTaggedTime(t *testing.T) {
	// An implicitly tagged time value, that happens to have an implicit
	// tag equal to a GENERALIZEDTIME, should still be parsed as a UTCTime.
	// (There's no "timeType" in fieldParameters to determine what type of
	// time should be expected when implicitly tagged.)
	der := []byte{0x30, 0x0f, 0x80 | 24, 0xd, '9', '1', '0', '5', '0', '6', '1', '6', '4', '5', '4', '0', 'Z'}
	var result implicitTaggedTimeTest
	if _, err := Unmarshal(der, &result); err != nil {
		t.Fatalf("Error while parsing: %s", err)
	}
	if expected := time.Date(1991, 05, 06, 16, 45, 40, 0, time.UTC); !result.Time.Equal(expected) {
		t.Errorf("Wrong result. Got %v, want %v", result.Time, expected)
	}
}

type truncatedExplicitTagTest struct {
	Test int `asn1:"explicit,tag:0"`
}

func TestTruncatedExplicitTag(t *testing.T) {
	// This crashed Unmarshal in the past. See #11154.
	der := []byte{
		0x30, // SEQUENCE
		0x02, // two bytes long
		0xa0, // context-specific, tag 0
		0x30, // 48 bytes long
	}

	var result truncatedExplicitTagTest
	if _, err := Unmarshal(der, &result); err == nil {
		t.Error("Unmarshal returned without error")
	}
}

type invalidUTF8Test struct {
	Str string `asn1:"utf8"`
}

func TestUnmarshalInvalidUTF8(t *testing.T) {
	data := []byte("0\x05\f\x03a\xc9c")
	var result invalidUTF8Test
	_, err := Unmarshal(data, &result)

	const expectedSubstring = "UTF"
	if err == nil {
		t.Fatal("Successfully unmarshaled invalid UTF-8 data")
	} else if !strings.Contains(err.Error(), expectedSubstring) {
		t.Fatalf("Expected error to mention %q but error was %q", expectedSubstring, err.Error())
	}
}
