package ber

import (
	"bytes"
	"encoding/asn1"
	"encoding/hex"
	"testing"
)

type marshalTest struct {
	in  interface{}
	out string // hex encoded
}

var marshalTests = []marshalTest{
	{asn1.ObjectIdentifier([]int{1, 2, 3, 4}), "06032a0304"},
	{asn1.ObjectIdentifier([]int{1, 2, 840, 133549, 1, 1, 5}), "06092a864888932d010105"},
	{asn1.ObjectIdentifier([]int{2, 100, 3}), "0603813403"},

	// Ensure large OID suboids are marshalled correctly
	// See marshal_64bit_test.go for none 32-bit compatible examples.
	{
		asn1.ObjectIdentifier([]int{1, 3, 6, 1, 2, 1, 31, 1, 1, 1, 1, 1090781219}),
		"060f2b060102011f010101018488908023",
	},
}

func TestMarshal(t *testing.T) {
	for i, test := range marshalTests {
		data, err := Marshal(test.in)
		if err != nil {
			t.Errorf("#%d failed: %s", i, err)
		}
		out, _ := hex.DecodeString(test.out)
		if !bytes.Equal(out, data) {
			t.Errorf("#%d got: %x want %x\n\t%q\n\t%q", i, data, out, data, out)

		}
	}
}
