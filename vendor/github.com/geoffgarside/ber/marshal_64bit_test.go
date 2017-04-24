// +build amd64

package ber

import "encoding/asn1"

func init() {
	marshalTests = append(marshalTests, marshalTest{
		asn1.ObjectIdentifier([]int{1, 3, 6, 1, 4, 1, 9, 10, 138, 1, 4, 1, 2, 1, 3221225473}),
		"06132b06010401090a810a01040102018c80808001",
	})
}
