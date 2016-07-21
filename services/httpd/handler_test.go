package httpd

import (
	"errors"
	"testing"

	"github.com/influxdata/kapacitor/auth"
)

func Test_RequiredPrilegeForHTTPMethod(t *testing.T) {
	testCases := []struct {
		m   string
		rp  auth.Privilege
		err error
	}{
		{
			m:   "GET",
			rp:  auth.ReadPrivilege,
			err: nil,
		},
		{
			m:   "get",
			rp:  auth.ReadPrivilege,
			err: nil,
		},
		{
			m:   "HEAD",
			rp:  auth.NoPrivileges,
			err: nil,
		},
		{
			m:   "head",
			rp:  auth.NoPrivileges,
			err: nil,
		},
		{
			m:   "OPTIONS",
			rp:  auth.NoPrivileges,
			err: nil,
		},
		{
			m:   "options",
			rp:  auth.NoPrivileges,
			err: nil,
		},
		{
			m:   "POST",
			rp:  auth.WritePrivilege,
			err: nil,
		},
		{
			m:   "post",
			rp:  auth.WritePrivilege,
			err: nil,
		},
		{
			m:   "PATCH",
			rp:  auth.WritePrivilege,
			err: nil,
		},
		{
			m:   "patch",
			rp:  auth.WritePrivilege,
			err: nil,
		},
		{
			m:   "DELETE",
			rp:  auth.DeletePrivilege,
			err: nil,
		},
		{
			m:   "delete",
			rp:  auth.DeletePrivilege,
			err: nil,
		},
		{
			m:   "PUT",
			err: errors.New(`unknown method "PUT"`),
		},
	}

	for _, tc := range testCases {
		got, err := requiredPrivilegeForHTTPMethod(tc.m)
		if err != nil {
			if tc.err == nil {
				t.Errorf("unexpected error: got %v", err)
			} else if tc.err.Error() != err.Error() {
				t.Errorf("unexpected error message: got %q exp %q", err.Error(), tc.err.Error())
			}
		} else {
			if tc.err != nil {
				t.Errorf("expected error: %q got nil", tc.err.Error())
				continue
			}
			if got != tc.rp {
				t.Errorf("unexpected required privilege: got %v exp %v", got, tc.rp)
			}
		}
	}
}
