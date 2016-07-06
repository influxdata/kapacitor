package auth_test

import (
	"errors"
	"testing"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/kapacitor/auth"
)

func Test_Privilege_String(t *testing.T) {
	testCases := []struct {
		p auth.Privilege
		s string
	}{
		{
			p: auth.NoPrivileges,
			s: "NO_PRIVILEGES",
		},
		{
			p: auth.ReadPrivilege,
			s: "read",
		},
		{
			p: auth.WritePrivilege,
			s: "write",
		},
		{
			p: auth.DeletePrivilege,
			s: "delete",
		},
		{
			p: auth.AllPrivileges,
			s: "ALL_PRIVILEGES",
		},
		{
			p: auth.AllPrivileges + 1,
			s: "UNKNOWN_PRIVILEGE",
		},
	}

	for _, tc := range testCases {
		if exp, got := tc.s, tc.p.String(); exp != got {
			t.Errorf("unexpected string value: got %s exp %s", got, exp)
		}
	}
}

func Test_Action_RequiredPrilege(t *testing.T) {
	testCases := []struct {
		a   auth.Action
		rp  auth.Privilege
		err error
	}{
		{
			a: auth.Action{
				Resource: "/",
				Method:   "GET",
			},
			rp:  auth.ReadPrivilege,
			err: nil,
		},
		{
			a: auth.Action{
				Resource: "/",
				Method:   "get",
			},
			rp:  auth.ReadPrivilege,
			err: nil,
		},
		{
			a: auth.Action{
				Resource: "/",
				Method:   "HEAD",
			},
			rp:  auth.NoPrivileges,
			err: nil,
		},
		{
			a: auth.Action{
				Resource: "/",
				Method:   "head",
			},
			rp:  auth.NoPrivileges,
			err: nil,
		},
		{
			a: auth.Action{
				Resource: "/",
				Method:   "OPTIONS",
			},
			rp:  auth.NoPrivileges,
			err: nil,
		},
		{
			a: auth.Action{
				Resource: "/",
				Method:   "options",
			},
			rp:  auth.NoPrivileges,
			err: nil,
		},
		{
			a: auth.Action{
				Resource: "/",
				Method:   "POST",
			},
			rp:  auth.WritePrivilege,
			err: nil,
		},
		{
			a: auth.Action{
				Resource: "/",
				Method:   "post",
			},
			rp:  auth.WritePrivilege,
			err: nil,
		},
		{
			a: auth.Action{
				Resource: "/",
				Method:   "PATCH",
			},
			rp:  auth.WritePrivilege,
			err: nil,
		},
		{
			a: auth.Action{
				Resource: "/",
				Method:   "patch",
			},
			rp:  auth.WritePrivilege,
			err: nil,
		},
		{
			a: auth.Action{
				Resource: "/",
				Method:   "DELETE",
			},
			rp:  auth.DeletePrivilege,
			err: nil,
		},
		{
			a: auth.Action{
				Resource: "/",
				Method:   "delete",
			},
			rp:  auth.DeletePrivilege,
			err: nil,
		},
		{
			a: auth.Action{
				Resource: "/",
				Method:   "PUT",
			},
			err: errors.New(`unknown method "PUT"`),
		},
	}

	for _, tc := range testCases {
		got, err := tc.a.RequiredPrivilege()
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
func Test_User_Name(t *testing.T) {
	u := auth.NewUser("username", nil, nil)
	if got := u.Name(); got != "username" {
		t.Errorf("unexpected username: got %s exp username", got)
	}
}

func Test_User_AuthorizeAction(t *testing.T) {
	testCases := []struct {
		username         string
		admin            bool
		actionPrivileges map[string]auth.Privilege
		action           auth.Action
		authorized       bool
		err              error
	}{
		{
			username: "bob",
			actionPrivileges: map[string]auth.Privilege{
				"/a/b/c": auth.WritePrivilege,
			},
			action: auth.Action{
				Resource: "/a/b/c",
				Method:   "POST",
			},
			authorized: true,
			err:        nil,
		},
		{
			username: "jim",
			actionPrivileges: map[string]auth.Privilege{
				"/a/b/": auth.WritePrivilege,
			},
			action: auth.Action{
				Resource: "/a/b/c",
				Method:   "POST",
			},
			authorized: true,
			err:        nil,
		},
		{
			username: "sue",
			actionPrivileges: map[string]auth.Privilege{
				"/a/b": auth.WritePrivilege,
			},
			action: auth.Action{
				Resource: "/a/b/c",
				Method:   "POST",
			},
			authorized: true,
			err:        nil,
		},
		{
			username: "sally",
			actionPrivileges: map[string]auth.Privilege{
				"/a/": auth.WritePrivilege,
			},
			action: auth.Action{
				Resource: "/a/b/c",
				Method:   "POST",
			},
			authorized: true,
			err:        nil,
		},
		{
			username: "alice",
			actionPrivileges: map[string]auth.Privilege{
				"/": auth.WritePrivilege,
			},
			action: auth.Action{
				Resource: "/a/b/c",
				Method:   "POST",
			},
			authorized: true,
			err:        nil,
		},
		{
			username: "nick",
			actionPrivileges: map[string]auth.Privilege{
				"/c/": auth.WritePrivilege,
			},
			action: auth.Action{
				Resource: "/a/b/c",
				Method:   "POST",
			},
			authorized: false,
			err:        errors.New(`user nick does not have "write" privilege for resource "/a/b/c"`),
		},
		{
			username: "annie",
			actionPrivileges: map[string]auth.Privilege{
				"/a/b/c/": auth.WritePrivilege,
			},
			action: auth.Action{
				Resource: "/a/b/c",
				Method:   "POST",
			},
			authorized: true,
			err:        nil,
		},
		{
			username: "fred",
			actionPrivileges: map[string]auth.Privilege{
				"/a/b/c": auth.ReadPrivilege,
			},
			action: auth.Action{
				Resource: "/a/b/c",
				Method:   "POST",
			},
			authorized: false,
			err:        errors.New(`user fred does not have "write" privilege for resource "/a/b/c"`),
		},
		{
			username: "phillip",
			actionPrivileges: map[string]auth.Privilege{
				"/a/b/c": auth.ReadPrivilege,
			},
			action: auth.Action{
				Resource: "a/b/c",
				Method:   "POST",
			},
			authorized: false,
			err:        errors.New(`invalid action resource: "a/b/c", must be an absolute path`),
		},
		{
			username: "andrew",
			actionPrivileges: map[string]auth.Privilege{
				"/a/b/c": auth.ReadPrivilege,
			},
			action: auth.Action{
				Resource: "/a/b/c",
				Method:   "HEAD",
			},
			authorized: true,
			err:        nil,
		},
		{
			username: "amy",
			actionPrivileges: map[string]auth.Privilege{
				"/": auth.WritePrivilege | auth.ReadPrivilege,
			},
			action: auth.Action{
				Resource: "/a/b/c",
				Method:   "POST",
			},
			authorized: true,
			err:        nil,
		},
		{
			username: "andy",
			actionPrivileges: map[string]auth.Privilege{
				"/": auth.WritePrivilege | auth.ReadPrivilege | auth.DeletePrivilege,
			},
			action: auth.Action{
				Resource: "/a/b/c",
				Method:   "POST",
			},
			authorized: true,
			err:        nil,
		},
		{
			username: "nicole",
			actionPrivileges: map[string]auth.Privilege{
				"/": auth.WritePrivilege | auth.DeletePrivilege,
			},
			action: auth.Action{
				Resource: "/a/b/c",
				Method:   "POST",
			},
			authorized: true,
			err:        nil,
		},
		{
			username: "nathan",
			actionPrivileges: map[string]auth.Privilege{
				"/": auth.AllPrivileges,
			},
			action: auth.Action{
				Resource: "/a/b/c",
				Method:   "POST",
			},
			authorized: true,
			err:        nil,
		},
		{
			username: "natalie",
			actionPrivileges: map[string]auth.Privilege{
				"/": auth.ReadPrivilege | auth.DeletePrivilege,
			},
			action: auth.Action{
				Resource: "/a/b/c",
				Method:   "POST",
			},
			authorized: false,
			err:        errors.New(`user natalie does not have "write" privilege for resource "/a/b/c"`),
		},
		{
			username: "katherine",
			actionPrivileges: map[string]auth.Privilege{
				"/": auth.NoPrivileges,
			},
			action: auth.Action{
				Resource: "/a/b/c",
				Method:   "POST",
			},
			authorized: false,
			err:        errors.New(`user katherine does not have "write" privilege for resource "/a/b/c"`),
		},
		{
			username: "ellie",
			actionPrivileges: map[string]auth.Privilege{
				"/": auth.WritePrivilege,
			},
			action: auth.Action{
				Resource: "/a/b/c",
				Method:   "PATCH",
			},
			authorized: true,
			err:        nil,
		},
		{
			username: "jackson",
			actionPrivileges: map[string]auth.Privilege{
				"/": auth.DeletePrivilege | auth.ReadPrivilege,
			},
			action: auth.Action{
				Resource: "/a/b/c",
				Method:   "PATCH",
			},
			authorized: false,
			err:        errors.New(`user jackson does not have "write" privilege for resource "/a/b/c"`),
		},
		{
			username: "anthony",
			actionPrivileges: map[string]auth.Privilege{
				"/": auth.NoPrivileges,
			},
			action: auth.Action{
				Resource: "/a/b/c",
				Method:   "PATCH",
			},
			authorized: false,
			err:        errors.New(`user anthony does not have "write" privilege for resource "/a/b/c"`),
		},
		{
			username: "john",
			actionPrivileges: map[string]auth.Privilege{
				"/": auth.DeletePrivilege,
			},
			action: auth.Action{
				Resource: "/a/b/c",
				Method:   "DELETE",
			},
			authorized: true,
			err:        nil,
		},
		{
			username: "joe",
			actionPrivileges: map[string]auth.Privilege{
				"/": auth.ReadPrivilege | auth.WritePrivilege,
			},
			action: auth.Action{
				Resource: "/a/b/c",
				Method:   "DELETE",
			},
			authorized: false,
			err:        errors.New(`user joe does not have "delete" privilege for resource "/a/b/c"`),
		},
		{
			username: "jimmy",
			actionPrivileges: map[string]auth.Privilege{
				"/": auth.NoPrivileges,
			},
			action: auth.Action{
				Resource: "/a/b/c",
				Method:   "DELETE",
			},
			authorized: false,
			err:        errors.New(`user jimmy does not have "delete" privilege for resource "/a/b/c"`),
		},
		{
			username: "alex",
			actionPrivileges: map[string]auth.Privilege{
				"/": auth.ReadPrivilege,
			},
			action: auth.Action{
				Resource: "/a/b/c",
				Method:   "GET",
			},
			authorized: true,
			err:        nil,
		},
		{
			username: "kevin",
			actionPrivileges: map[string]auth.Privilege{
				"/": auth.DeletePrivilege | auth.WritePrivilege,
			},
			action: auth.Action{
				Resource: "/a/b/c",
				Method:   "GET",
			},
			authorized: false,
			err:        errors.New(`user kevin does not have "read" privilege for resource "/a/b/c"`),
		},
		{
			username: "katie",
			actionPrivileges: map[string]auth.Privilege{
				"/": auth.NoPrivileges,
			},
			action: auth.Action{
				Resource: "/a/b/c",
				Method:   "GET",
			},
			authorized: false,
			err:        errors.New(`user katie does not have "read" privilege for resource "/a/b/c"`),
		},
		{
			username: "spencer",
			actionPrivileges: map[string]auth.Privilege{
				"/a":       auth.NoPrivileges,
				"/a/b/c/d": auth.AllPrivileges,
				"/b":       auth.AllPrivileges,
				"/c":       auth.AllPrivileges,
			},
			action: auth.Action{
				Resource: "/a/b/c",
				Method:   "GET",
			},
			authorized: false,
			err:        errors.New(`user spencer does not have "read" privilege for resource "/a/b/c"`),
		},
		{
			username: "susan",
			actionPrivileges: map[string]auth.Privilege{
				"/":      auth.AllPrivileges,
				"/a/b/c": auth.NoPrivileges,
			},
			action: auth.Action{
				Resource: "/a/b/c",
				Method:   "GET",
			},
			authorized: false,
			err:        errors.New(`user susan does not have "read" privilege for resource "/a/b/c"`),
		},
		{
			username: "timothy",
			actionPrivileges: map[string]auth.Privilege{
				"/":      auth.AllPrivileges,
				"/a/b/c": auth.WritePrivilege,
			},
			action: auth.Action{
				Resource: "/a/b/c",
				Method:   "GET",
			},
			authorized: false,
			err:        errors.New(`user timothy does not have "read" privilege for resource "/a/b/c"`),
		},
		{
			username: "alexander",
			actionPrivileges: map[string]auth.Privilege{
				"/":      auth.AllPrivileges,
				"/a/b/c": auth.WritePrivilege | auth.ReadPrivilege,
			},
			action: auth.Action{
				Resource: "/a/b/c",
				Method:   "GET",
			},
			authorized: true,
			err:        nil,
		},
		{
			username: "tim",
			action: auth.Action{
				Resource: "/a/b/c",
				Method:   "GET",
			},
			authorized: false,
			err:        errors.New(`user tim does not have "read" privilege for resource "/a/b/c"`),
		},
		{
			username: "slim",
			action: auth.Action{
				Resource: "/a/b/c",
				Method:   "OPTIONS",
			},
			authorized: true,
			err:        nil,
		},
		{
			username: "chase",
			action: auth.Action{
				Resource: "/a/b/c",
				Method:   "OPTIONS",
			},
			authorized: true,
			err:        nil,
		},
		{
			username: "hacker",
			action: auth.Action{
				Resource: "/a/b/c",
				Method:   "PUT",
			},
			authorized: false,
			err:        errors.New("cannot authorize invalid action: unknown method \"PUT\""),
		},
		{
			username: "cleverbob",
			actionPrivileges: map[string]auth.Privilege{
				"/a/d/e/f": auth.ReadPrivilege,
			},
			action: auth.Action{
				Resource: "/a/b/c/../../d/e/f",
				Method:   "GET",
			},
			authorized: true,
			err:        nil,
		},
		{
			username: "hackerbob",
			actionPrivileges: map[string]auth.Privilege{
				"/a/b": auth.WritePrivilege,
			},
			action: auth.Action{
				Resource: "/a/b/c/../../d/e/f",
				Method:   "POST",
			},
			authorized: false,
			err:        errors.New(`user hackerbob does not have "write" privilege for resource "/a/b/c/../../d/e/f"`),
		},
		{
			username: "hackerjim",
			actionPrivileges: map[string]auth.Privilege{
				"/a/b": auth.WritePrivilege,
			},
			action: auth.Action{
				Resource: "/a/b/c/../../../../d/e/f",
				Method:   "POST",
			},
			authorized: false,
			err:        errors.New(`user hackerjim does not have "write" privilege for resource "/a/b/c/../../../../d/e/f"`),
		},
		{
			username: "admin",
			admin:    true,
			action: auth.Action{
				Resource: "/a/b/c",
				Method:   "POST",
			},
			authorized: true,
			err:        nil,
		},
	}
	for _, tc := range testCases {
		var u auth.User
		if tc.admin {
			u = auth.AdminUser
		} else {
			u = auth.NewUser(tc.username, tc.actionPrivileges, nil)
		}
		authorized, err := u.AuthorizeAction(tc.action)
		if err != nil {
			if tc.err == nil {
				t.Errorf("%s: unexpected error authorizing action: got %q", tc.username, err.Error())
			} else if err.Error() != tc.err.Error() {
				t.Errorf("%s: unexpected error message: got %q exp %q", tc.username, err.Error(), tc.err.Error())
			}
		} else {
			if tc.err != nil {
				t.Errorf("%s: expected error authorizing action: %q", tc.username, tc.err.Error())
			}
			if authorized != tc.authorized {
				t.Errorf("%s: AUTH BREACH: got %t exp %t", tc.username, authorized, tc.authorized)
			}
		}
	}
}

func Test_User_AuthorizeDB(t *testing.T) {
	testCases := []struct {
		username     string
		admin        bool
		dbPrivileges map[string]influxql.Privilege
		privilege    influxql.Privilege
		database     string
		authorized   bool
	}{
		{
			username: "bob",
			dbPrivileges: map[string]influxql.Privilege{
				"db": influxql.ReadPrivilege,
			},
			privilege:  influxql.ReadPrivilege,
			database:   "db",
			authorized: true,
		},
		{
			username:   "jim",
			privilege:  influxql.ReadPrivilege,
			database:   "db",
			authorized: false,
		},
		{
			username: "sue",
			dbPrivileges: map[string]influxql.Privilege{
				"db": influxql.ReadPrivilege,
			},
			privilege:  influxql.WritePrivilege,
			database:   "db",
			authorized: false,
		},
		{
			username: "tim",
			dbPrivileges: map[string]influxql.Privilege{
				"dbname": influxql.ReadPrivilege,
			},
			privilege:  influxql.ReadPrivilege,
			database:   "db",
			authorized: false,
		},
		{
			username: "slim",
			dbPrivileges: map[string]influxql.Privilege{
				"dbname": influxql.AllPrivileges,
			},
			privilege:  influxql.ReadPrivilege,
			database:   "dbname",
			authorized: true,
		},
		{
			username: "jill",
			dbPrivileges: map[string]influxql.Privilege{
				"dbname": influxql.AllPrivileges,
			},
			privilege:  influxql.WritePrivilege,
			database:   "dbname",
			authorized: true,
		},
		{
			username:   "admin",
			admin:      true,
			privilege:  influxql.WritePrivilege,
			database:   "dbname",
			authorized: true,
		},
	}
	for _, tc := range testCases {
		var u auth.User
		if tc.admin {
			u = auth.AdminUser
		} else {
			u = auth.NewUser(tc.username, nil, tc.dbPrivileges)
		}
		authorized := u.AuthorizeDB(tc.privilege, tc.database)
		if authorized != tc.authorized {
			t.Errorf("%s: AUTH BREACH: got %t exp %t", tc.username, authorized, tc.authorized)
		}
	}
}
