package snmpgo

import (
	"bytes"
	"encoding/asn1"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strconv"
	"strings"

	"github.com/geoffgarside/ber"
)

type Variable interface {
	// Return a BigInt representation of this Variable if such a representation exists
	BigInt() (*big.Int, error)
	// Return a string representation of this Variable
	String() string
	// Return a string of type
	Type() string
	Marshal() ([]byte, error)
	Unmarshal([]byte) (rest []byte, err error)
}

type Integer struct {
	Value int32
}

func (v *Integer) BigInt() (*big.Int, error) {
	return big.NewInt(int64(v.Value)), nil
}

func (v *Integer) String() string {
	return strconv.FormatInt(int64(v.Value), 10)
}

func (v *Integer) Type() string {
	return "Integer"
}

func (v *Integer) Marshal() ([]byte, error) {
	return asn1.Marshal(v.Value)
}

func (v *Integer) Unmarshal(b []byte) (rest []byte, err error) {
	return ber.Unmarshal(b, &v.Value)
}

func NewInteger(i int32) *Integer {
	return &Integer{i}
}

type OctetString struct {
	Value []byte
}

func (v *OctetString) BigInt() (*big.Int, error) {
	return nil, UnsupportedOperation
}

func (v *OctetString) String() string {
	for _, c := range v.Value {
		switch {
		case c >= 0x20 && c <= 0x7e:
			// printable character including space
		case c >= 0x09 && c <= 0x0d:
			// '\t', '\n', '\v', '\f', '\r'
		default:
			return toHexStr(v.Value, ":")
		}
	}
	return string(v.Value)
}

func (v *OctetString) Type() string {
	return "OctetString"
}

func (v *OctetString) Marshal() ([]byte, error) {
	return asn1.Marshal(v.Value)
}

func (v *OctetString) Unmarshal(b []byte) (rest []byte, err error) {
	return unmarshalString(b, tagOctetString, func(s []byte) { v.Value = s })
}

func NewOctetString(b []byte) *OctetString {
	return &OctetString{b}
}

type Null struct{}

func (v *Null) BigInt() (*big.Int, error) {
	return nil, UnsupportedOperation
}

func (v *Null) String() string {
	return ""
}

func (v *Null) Type() string {
	return "Null"
}

func (v *Null) Marshal() ([]byte, error) {
	return []byte{tagNull, 0}, nil
}

func (v *Null) Unmarshal(b []byte) (rest []byte, err error) {
	return unmarshalEmpty(b, tagNull)
}

func NewNull() *Null {
	return &Null{}
}

type Oid struct {
	Value asn1.ObjectIdentifier
}

func (v *Oid) BigInt() (*big.Int, error) {
	return nil, UnsupportedOperation
}

func (v *Oid) String() string {
	return v.Value.String()
}

func (v *Oid) Type() string {
	return "Oid"
}

func (v *Oid) Marshal() ([]byte, error) {
	return asn1.Marshal(v.Value)
}

func (v *Oid) Unmarshal(b []byte) (rest []byte, err error) {
	var i asn1.ObjectIdentifier
	rest, err = ber.Unmarshal(b, &i)
	if err == nil {
		v.Value = i
	}
	return
}

// Returns true if this OID contains the specified OID
func (v *Oid) Contains(o *Oid) bool {
	if o == nil || len(v.Value) < len(o.Value) {
		return false
	}
	for i := 0; i < len(o.Value); i++ {
		if v.Value[i] != o.Value[i] {
			return false
		}
	}
	return true
}

// Returns 0 this OID is equal to the specified OID,
// -1 this OID is lexicographically less than the specified OID,
// 1 this OID is lexicographically greater than the specified OID
func (v *Oid) Compare(o *Oid) int {
	if o != nil {
		vl := len(v.Value)
		ol := len(o.Value)
		for i := 0; i < vl; i++ {
			if ol <= i || v.Value[i] > o.Value[i] {
				return 1
			} else if v.Value[i] < o.Value[i] {
				return -1
			}
		}
		if ol == vl {
			return 0
		}
	}
	return -1
}

// Returns true if this OID is same the specified OID
func (v *Oid) Equal(o *Oid) bool {
	if o == nil {
		return false
	}
	return v.Value.Equal(o.Value)
}

// Returns Oid with additional sub-ids
func (v *Oid) AppendSubIds(subs []int) (*Oid, error) {
	buf := bytes.NewBufferString(v.String())
	for _, i := range subs {
		buf.WriteString(".")
		buf.WriteString(strconv.Itoa(i))
	}
	return NewOid(buf.String())
}

func NewOid(s string) (oid *Oid, err error) {
	subids := strings.Split(s, ".")

	// leading dot
	if subids[0] == "" {
		subids = subids[1:]
	}

	// RFC2578 Section 3.5
	if len(subids) > 128 {
		return nil, &ArgumentError{
			Value:   s,
			Message: "The sub-identifiers in an OID is up to 128",
		}
	}

	o := make(asn1.ObjectIdentifier, len(subids))
	for i, v := range subids {
		o[i], err = strconv.Atoi(v)
		if err != nil || o[i] < 0 || int64(o[i]) > math.MaxUint32 {
			return nil, &ArgumentError{
				Value:   s,
				Message: fmt.Sprintf("The sub-identifiers is range %d..%d", 0, int64(math.MaxUint32)),
			}
		}
	}

	if len(o) > 0 && o[0] > 2 {
		return nil, &ArgumentError{
			Value:   s,
			Message: "The first sub-identifier is range 0..2",
		}
	}

	// ISO/IEC 8825 Section 8.19.4
	if len(o) < 2 {
		return nil, &ArgumentError{
			Value:   s,
			Message: "The first and second sub-identifier is required",
		}
	}

	if o[0] < 2 && o[1] >= 40 {
		return nil, &ArgumentError{
			Value:   s,
			Message: "The second sub-identifier is range 0..39",
		}
	}

	return &Oid{o}, nil
}

// MustNewOid is like NewOid but panics if argument cannot be parsed
func MustNewOid(s string) *Oid {
	if oid, err := NewOid(s); err != nil {
		panic(`snmpgo.MustNewOid: ` + err.Error())
	} else {
		return oid
	}
}

type Oids []*Oid

// Sort a Oid list
func (o Oids) Sort() Oids {
	c := make(Oids, len(o))
	copy(c, o)
	sort.Sort(sortableOids{c})
	return c
}

func (o Oids) uniq(comp func(a, b *Oid) bool) Oids {
	var before *Oid
	c := make(Oids, 0, len(o))
	for _, oid := range o {
		if !comp(before, oid) {
			before = oid
			c = append(c, oid)
		}
	}
	return c
}

// Filter out adjacent OID list
func (o Oids) Uniq() Oids {
	return o.uniq(func(a, b *Oid) bool {
		if b == nil {
			return a == nil
		} else {
			return b.Equal(a)
		}
	})
}

// Filter out adjacent OID list with the same prefix
func (o Oids) UniqBase() Oids {
	return o.uniq(func(a, b *Oid) bool {
		if b == nil {
			return a == nil
		} else {
			return b.Contains(a)
		}
	})
}

type sortableOids struct {
	Oids
}

func (o sortableOids) Len() int {
	return len(o.Oids)
}

func (o sortableOids) Swap(i, j int) {
	o.Oids[i], o.Oids[j] = o.Oids[j], o.Oids[i]
}

func (o sortableOids) Less(i, j int) bool {
	return o.Oids[i] != nil && o.Oids[i].Compare(o.Oids[j]) < 1
}

func NewOids(s []string) (oids Oids, err error) {
	for _, l := range s {
		o, e := NewOid(l)
		if e != nil {
			return nil, e
		}
		oids = append(oids, o)
	}
	return
}

type Ipaddress struct {
	OctetString
}

func (v *Ipaddress) BigInt() (*big.Int, error) {
	var t uint32
	for i, b := range v.Value {
		t = t + (uint32(b) << uint(24-8*i))
	}
	return big.NewInt(int64(t)), nil
}

func (v *Ipaddress) String() string {
	s := make([]string, len(v.Value))
	for i, b := range v.Value {
		s[i] = strconv.Itoa(int(b))
	}
	return strings.Join(s, ".")
}

func (v *Ipaddress) Type() string {
	return "Ipaddress"
}

func (v *Ipaddress) Marshal() ([]byte, error) {
	b, err := asn1.Marshal(v.Value)
	if err == nil {
		b[0] = tagIpaddress
	}
	return b, err
}

func (v *Ipaddress) Unmarshal(b []byte) (rest []byte, err error) {
	return unmarshalString(b, tagIpaddress, func(s []byte) { v.Value = s })
}

func NewIpaddress(a, b, c, d byte) *Ipaddress {
	return &Ipaddress{OctetString{[]byte{a, b, c, d}}}
}

type Counter32 struct {
	Value uint32
}

func (v *Counter32) BigInt() (*big.Int, error) {
	return big.NewInt(int64(v.Value)), nil
}

func (v *Counter32) String() string {
	return strconv.FormatInt(int64(v.Value), 10)
}

func (v *Counter32) Type() string {
	return "Counter32"
}

func (v *Counter32) Marshal() ([]byte, error) {
	b, err := asn1.Marshal(int64(v.Value))
	if err == nil {
		b[0] = tagCounter32
	}
	return b, err
}

func (v *Counter32) Unmarshal(b []byte) (rest []byte, err error) {
	return unmarshalInt(b, tagCounter32, func(s *big.Int) { v.Value = uint32(s.Int64()) })
}

func NewCounter32(i uint32) *Counter32 {
	return &Counter32{i}
}

type Gauge32 struct {
	Counter32
}

func (v *Gauge32) Type() string {
	return "Gauge32"
}

func (v *Gauge32) Marshal() ([]byte, error) {
	b, err := asn1.Marshal(int64(v.Value))
	if err == nil {
		b[0] = tagGauge32
	}
	return b, err
}

func (v *Gauge32) Unmarshal(b []byte) (rest []byte, err error) {
	return unmarshalInt(b, tagGauge32, func(s *big.Int) { v.Value = uint32(s.Int64()) })
}

func NewGauge32(i uint32) *Gauge32 {
	return &Gauge32{Counter32{i}}
}

type TimeTicks struct {
	Counter32
}

func (v *TimeTicks) Type() string {
	return "TimeTicks"
}

func (v *TimeTicks) Marshal() ([]byte, error) {
	b, err := asn1.Marshal(int64(v.Value))
	if err == nil {
		b[0] = tagTimeTicks
	}
	return b, err
}

func (v *TimeTicks) Unmarshal(b []byte) (rest []byte, err error) {
	return unmarshalInt(b, tagTimeTicks, func(s *big.Int) { v.Value = uint32(s.Int64()) })
}

func NewTimeTicks(i uint32) *TimeTicks {
	return &TimeTicks{Counter32{i}}
}

type Opaque struct {
	OctetString
}

func (v *Opaque) String() string {
	return toHexStr(v.Value, ":")
}

func (v *Opaque) Type() string {
	return "Opaque"
}

func (v *Opaque) Marshal() ([]byte, error) {
	b, err := asn1.Marshal(v.Value)
	if err == nil {
		b[0] = tagOpaque
	}
	return b, err
}

func (v *Opaque) Unmarshal(b []byte) (rest []byte, err error) {
	return unmarshalString(b, tagOpaque, func(s []byte) { v.Value = s })
}

func NewOpaque(b []byte) *Opaque {
	return &Opaque{OctetString{b}}
}

type Counter64 struct {
	Value uint64
}

func (v *Counter64) BigInt() (*big.Int, error) {
	return big.NewInt(0).SetUint64(v.Value), nil
}

func (v *Counter64) String() string {
	return strconv.FormatUint(v.Value, 10)
}

func (v *Counter64) Type() string {
	return "Counter64"
}

func (v *Counter64) Marshal() ([]byte, error) {
	i := big.NewInt(0).SetUint64(v.Value)
	b, err := asn1.Marshal(i)
	if err == nil {
		b[0] = tagCounter64
	}
	return b, err
}

func (v *Counter64) Unmarshal(b []byte) (rest []byte, err error) {
	return unmarshalInt(b, tagCounter64, func(s *big.Int) { v.Value = s.Uint64() })
}

func NewCounter64(i uint64) *Counter64 {
	return &Counter64{i}
}

type NoSucheObject struct {
	Null
}

func (v *NoSucheObject) Type() string {
	return "NoSucheObject"
}

func (v *NoSucheObject) Marshal() ([]byte, error) {
	return []byte{tagNoSucheObject, 0}, nil
}

func (v *NoSucheObject) Unmarshal(b []byte) (rest []byte, err error) {
	return unmarshalEmpty(b, tagNoSucheObject)
}

func NewNoSucheObject() *NoSucheObject {
	return &NoSucheObject{Null{}}
}

type NoSucheInstance struct {
	Null
}

func (v *NoSucheInstance) Type() string {
	return "NoSucheInstance"
}

func (v *NoSucheInstance) Marshal() ([]byte, error) {
	return []byte{tagNoSucheInstance, 0}, nil
}

func (v *NoSucheInstance) Unmarshal(b []byte) (rest []byte, err error) {
	return unmarshalEmpty(b, tagNoSucheInstance)
}

func NewNoSucheInstance() *NoSucheInstance {
	return &NoSucheInstance{Null{}}
}

type EndOfMibView struct {
	Null
}

func (v *EndOfMibView) Type() string {
	return "EndOfMibView"
}

func (v *EndOfMibView) Marshal() ([]byte, error) {
	return []byte{tagEndOfMibView, 0}, nil
}

func (v *EndOfMibView) Unmarshal(b []byte) (rest []byte, err error) {
	return unmarshalEmpty(b, tagEndOfMibView)
}

func NewEndOfMibView() *EndOfMibView {
	return &EndOfMibView{Null{}}
}

func unmarshalVariable(b []byte) (v Variable, rest []byte, err error) {
	var raw asn1.RawValue
	rest, err = ber.Unmarshal(b, &raw)
	if err != nil {
		return
	}

	switch raw.Class {
	case classUniversal:
		switch raw.Tag {
		case tagInteger:
			var u Integer
			v = &u
		case tagOctetString:
			var u OctetString
			v = &u
		case tagNull:
			var u Null
			v = &u
		case tagObjectIdentifier:
			var u Oid
			v = &u
		}
	case classApplication:
		switch raw.Tag {
		case tagIpaddress & tagMask:
			var u Ipaddress
			v = &u
		case tagCounter32 & tagMask:
			var u Counter32
			v = &u
		case tagGauge32 & tagMask:
			var u Gauge32
			v = &u
		case tagTimeTicks & tagMask:
			var u TimeTicks
			v = &u
		case tagOpaque & tagMask:
			var u Opaque
			v = &u
		case tagCounter64 & tagMask:
			var u Counter64
			v = &u
		}
	case classContextSpecific:
		switch raw.Tag {
		case tagNoSucheObject & tagMask:
			var u NoSucheObject
			v = &u
		case tagNoSucheInstance & tagMask:
			var u NoSucheInstance
			v = &u
		case tagEndOfMibView & tagMask:
			var u EndOfMibView
			v = &u
		}
	}

	if v != nil {
		rest, err = v.Unmarshal(b)
		if err == nil {
			return
		}
	} else {
		err = asn1.StructuralError{fmt.Sprintf(
			"Unknown ASN.1 object : %s", toHexStr(b, " "))}
	}

	return nil, nil, err
}

func validateUnmarshal(b []byte, tag byte) error {
	if len(b) < 1 {
		return asn1.StructuralError{"No bytes"}
	}
	if b[0] != tag {
		return asn1.StructuralError{fmt.Sprintf(
			"Invalid ASN.1 object - expected [%02x], actual [%02x] : %s",
			tag, b[0], toHexStr(b, " "))}
	}
	return nil
}

func unmarshalEmpty(b []byte, tag byte) (rest []byte, err error) {
	err = validateUnmarshal(b, tag)
	if err != nil {
		return nil, err
	}

	var raw asn1.RawValue
	return ber.Unmarshal(b, &raw)
}

func unmarshalInt(b []byte, tag byte, setter func(*big.Int)) (rest []byte, err error) {
	err = validateUnmarshal(b, tag)
	if err != nil {
		return nil, err
	}

	temp := b[0]
	b[0] = tagInteger
	var i *big.Int
	rest, err = ber.Unmarshal(b, &i)
	if err == nil {
		setter(i)
	}
	b[0] = temp
	return
}

func unmarshalString(b []byte, tag byte, setter func([]byte)) (rest []byte, err error) {
	err = validateUnmarshal(b, tag)
	if err != nil {
		return nil, err
	}

	temp := b[0]
	b[0] = tagOctetString
	var s []byte
	rest, err = ber.Unmarshal(b, &s)
	if err == nil {
		setter(s)
	}
	b[0] = temp
	return
}
