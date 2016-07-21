package auth_test

import (
	"errors"
	"testing"

	"github.com/influxdata/kapacitor/auth"
)

func Test_Privilege_String(t *testing.T) {
	testCases := []struct {
		p auth.Privilege
		s string
	}{
		{
			p: auth.NoPrivileges,
			s: "none",
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
			s: "all",
		},
		{
			p: auth.AllPrivileges + 1,
			s: "unknown",
		},
	}

	for _, tc := range testCases {
		if exp, got := tc.s, tc.p.String(); exp != got {
			t.Errorf("unexpected string value: got %s exp %s", got, exp)
		}
	}
}

func Test_NewUser(t *testing.T) {
	privs := map[string][]auth.Privilege{
		"/simple/path/":               []auth.Privilege{auth.ReadPrivilege, auth.WritePrivilege},
		"/../messy/./path":            []auth.Privilege{auth.DeletePrivilege, auth.WritePrivilege},
		"/another/path/and/some/more": []auth.Privilege{auth.AllPrivileges},
		"/blocked/path":               []auth.Privilege{auth.NoPrivileges},
	}
	hash := []byte("hash")
	u := auth.NewUser("username", hash, false, privs)

	if exp, got := "username", u.Name(); got != exp {
		t.Errorf("unexpected username: got %s exp %s", got, exp)
	}
	if exp, got := false, u.IsAdmin(); got != exp {
		t.Errorf("unexpected admin: got %t exp %t", got, exp)
	}

	// Manipulate hash to test user has its own copy
	hash[0] = 'b'
	if exp, got := "hash", string(u.Hash()); got != exp {
		t.Errorf("unexpected hash: got %s exp %s", got, exp)
	}

	// Manipulate privileges to test user has its own copy
	privs["/blocked/path"] = []auth.Privilege{auth.AllPrivileges}

	expPrivileges := map[string][]auth.Privilege{
		"/simple/path":                []auth.Privilege{auth.ReadPrivilege, auth.WritePrivilege},
		"/messy/path":                 []auth.Privilege{auth.DeletePrivilege, auth.WritePrivilege},
		"/another/path/and/some/more": []auth.Privilege{auth.AllPrivileges},
		"/blocked/path":               []auth.Privilege{auth.NoPrivileges},
	}
	gotPrivileges := u.Privileges()
	if got, exp := len(gotPrivileges), len(expPrivileges); got != exp {
		t.Errorf("unexpected privileges count: got %d exp %d", got, exp)
	}
	for r, expPs := range expPrivileges {
		gotPs, ok := gotPrivileges[r]
		if !ok {
			t.Errorf("missing privilege: %s", r)
			continue
		}
		if got, exp := len(gotPs), len(expPs); got != exp {
			t.Errorf("unexpected privileges list count for resource %s: got %d exp %d", r, got, exp)
		}

		for _, ep := range expPs {
			found := false
			for _, gp := range gotPs {
				if ep == gp {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("missing privilege %v from list for resource %s", ep, r)
			}
		}
	}

	a := auth.NewUser("admin", nil, true, nil)
	if exp, got := "admin", a.Name(); got != exp {
		t.Errorf("unexpected username: got %s exp %s", got, exp)
	}
	if exp, got := true, a.IsAdmin(); got != exp {
		t.Errorf("unexpected admin: got %t exp %t", got, exp)
	}
}

func Test_User_AuthorizeAction(t *testing.T) {
	testCases := []struct {
		username   string
		admin      bool
		privileges map[string][]auth.Privilege
		action     auth.Action
		authorized bool
		err        error
	}{
		{
			username: "bob",
			privileges: map[string][]auth.Privilege{
				"/a/b/c": []auth.Privilege{auth.WritePrivilege},
			},
			action: auth.Action{
				Resource:  "/a/b/c",
				Privilege: auth.WritePrivilege,
			},
			authorized: true,
			err:        nil,
		},
		{
			username: "jim",
			privileges: map[string][]auth.Privilege{
				"/a/b/": []auth.Privilege{auth.WritePrivilege},
			},
			action: auth.Action{
				Resource:  "/a/b/c",
				Privilege: auth.WritePrivilege,
			},
			authorized: true,
			err:        nil,
		},
		{
			username: "sue",
			privileges: map[string][]auth.Privilege{
				"/a/b": []auth.Privilege{auth.WritePrivilege},
			},
			action: auth.Action{
				Resource:  "/a/b/c",
				Privilege: auth.WritePrivilege,
			},
			authorized: true,
			err:        nil,
		},
		{
			username: "sue",
			privileges: map[string][]auth.Privilege{
				"/b": []auth.Privilege{auth.WritePrivilege},
				"/c": []auth.Privilege{auth.WritePrivilege},
				"/d": []auth.Privilege{auth.WritePrivilege},
				"/a": []auth.Privilege{auth.WritePrivilege},
			},
			action: auth.Action{
				Resource:  "/a/b/c",
				Privilege: auth.WritePrivilege,
			},
			authorized: true,
			err:        nil,
		},
		{
			username: "sally",
			privileges: map[string][]auth.Privilege{
				"/a/": []auth.Privilege{auth.WritePrivilege},
			},
			action: auth.Action{
				Resource:  "/a/b/c",
				Privilege: auth.WritePrivilege,
			},
			authorized: true,
			err:        nil,
		},
		{
			username: "alice",
			privileges: map[string][]auth.Privilege{
				"/": []auth.Privilege{auth.WritePrivilege},
			},
			action: auth.Action{
				Resource:  "/a/b/c",
				Privilege: auth.WritePrivilege,
			},
			authorized: true,
			err:        nil,
		},
		{
			username: "nick",
			privileges: map[string][]auth.Privilege{
				"/c/": []auth.Privilege{auth.WritePrivilege},
			},
			action: auth.Action{
				Resource:  "/a/b/c",
				Privilege: auth.WritePrivilege,
			},
			authorized: false,
			err:        errors.New(`user nick does not have "write" privilege for resource "/a/b/c"`),
		},
		{
			username: "annie",
			privileges: map[string][]auth.Privilege{
				"/a/b/c/": []auth.Privilege{auth.WritePrivilege},
			},
			action: auth.Action{
				Resource:  "/a/b/c",
				Privilege: auth.WritePrivilege,
			},
			authorized: true,
			err:        nil,
		},
		{
			username: "fred",
			privileges: map[string][]auth.Privilege{
				"/a/b/c": []auth.Privilege{auth.ReadPrivilege},
			},
			action: auth.Action{
				Resource:  "/a/b/c",
				Privilege: auth.WritePrivilege,
			},
			authorized: false,
			err:        errors.New(`user fred does not have "write" privilege for resource "/a/b/c"`),
		},
		{
			username: "phillip",
			privileges: map[string][]auth.Privilege{
				"/a/b/c": []auth.Privilege{auth.ReadPrivilege},
			},
			action: auth.Action{
				Resource:  "a/b/c",
				Privilege: auth.WritePrivilege,
			},
			authorized: false,
			err:        errors.New(`invalid action resource: "a/b/c", must be an absolute path`),
		},
		{
			username: "amy",
			privileges: map[string][]auth.Privilege{
				"/": []auth.Privilege{auth.WritePrivilege, auth.ReadPrivilege},
			},
			action: auth.Action{
				Resource:  "/a/b/c",
				Privilege: auth.WritePrivilege,
			},
			authorized: true,
			err:        nil,
		},
		{
			username: "andy",
			privileges: map[string][]auth.Privilege{
				"/": []auth.Privilege{auth.WritePrivilege, auth.ReadPrivilege, auth.DeletePrivilege},
			},
			action: auth.Action{
				Resource:  "/a/b/c",
				Privilege: auth.WritePrivilege,
			},
			authorized: true,
			err:        nil,
		},
		{
			username: "nicole",
			privileges: map[string][]auth.Privilege{
				"/": []auth.Privilege{auth.WritePrivilege, auth.DeletePrivilege},
			},
			action: auth.Action{
				Resource:  "/a/b/c",
				Privilege: auth.WritePrivilege,
			},
			authorized: true,
			err:        nil,
		},
		{
			username: "nathan",
			privileges: map[string][]auth.Privilege{
				"/": []auth.Privilege{auth.AllPrivileges},
			},
			action: auth.Action{
				Resource:  "/a/b/c",
				Privilege: auth.WritePrivilege,
			},
			authorized: true,
			err:        nil,
		},
		{
			username: "natalie",
			privileges: map[string][]auth.Privilege{
				"/": []auth.Privilege{auth.ReadPrivilege, auth.DeletePrivilege},
			},
			action: auth.Action{
				Resource:  "/a/b/c",
				Privilege: auth.WritePrivilege,
			},
			authorized: false,
			err:        errors.New(`user natalie does not have "write" privilege for resource "/a/b/c"`),
		},
		{
			username: "katherine",
			privileges: map[string][]auth.Privilege{
				"/": []auth.Privilege{auth.NoPrivileges},
			},
			action: auth.Action{
				Resource:  "/a/b/c",
				Privilege: auth.WritePrivilege,
			},
			authorized: false,
			err:        errors.New(`user katherine does not have "write" privilege for resource "/a/b/c"`),
		},
		{
			username: "cleverbob",
			privileges: map[string][]auth.Privilege{
				"/a/d/e/f": []auth.Privilege{auth.ReadPrivilege},
			},
			action: auth.Action{
				Resource:  "/a/b/c/../../d/e/f",
				Privilege: auth.ReadPrivilege,
			},
			authorized: true,
			err:        nil,
		},
		{
			username: "hackerbob",
			privileges: map[string][]auth.Privilege{
				"/a/b": []auth.Privilege{auth.WritePrivilege},
			},
			action: auth.Action{
				Resource:  "/a/b/c/../../d/e/f",
				Privilege: auth.WritePrivilege,
			},
			authorized: false,
			err:        errors.New(`user hackerbob does not have "write" privilege for resource "/a/b/c/../../d/e/f"`),
		},
		{
			username: "hackerjim",
			privileges: map[string][]auth.Privilege{
				"/a/b": []auth.Privilege{auth.WritePrivilege},
			},
			action: auth.Action{
				Resource:  "/a/b/c/../../../../d/e/f",
				Privilege: auth.WritePrivilege,
			},
			authorized: false,
			err:        errors.New(`user hackerjim does not have "write" privilege for resource "/a/b/c/../../../../d/e/f"`),
		},
		{
			username: "admin",
			admin:    true,
			action: auth.Action{
				Resource:  "/a/b/c",
				Privilege: auth.WritePrivilege,
			},
			authorized: true,
			err:        nil,
		},
		{
			username: "susan",
			privileges: map[string][]auth.Privilege{
				"/": []auth.Privilege{auth.WritePrivilege, auth.ReadPrivilege, auth.DeletePrivilege},
			},
			action: auth.Action{
				Resource:  "/a/b/c",
				Privilege: auth.ReadPrivilege,
			},
			authorized: true,
			err:        nil,
		},
		{
			username: "ellie",
			privileges: map[string][]auth.Privilege{
				"/": []auth.Privilege{auth.WritePrivilege, auth.DeletePrivilege},
			},
			action: auth.Action{
				Resource:  "/a/b/c",
				Privilege: auth.ReadPrivilege,
			},
			authorized: false,
			err:        errors.New(`user ellie does not have "read" privilege for resource "/a/b/c"`),
		},
		{
			username: "spencer",
			privileges: map[string][]auth.Privilege{
				"/": []auth.Privilege{auth.ReadPrivilege, auth.DeletePrivilege},
			},
			action: auth.Action{
				Resource:  "/a/b/c",
				Privilege: auth.ReadPrivilege,
			},
			authorized: true,
			err:        nil,
		},
		{
			username: "alexander",
			privileges: map[string][]auth.Privilege{
				"/": []auth.Privilege{auth.AllPrivileges},
			},
			action: auth.Action{
				Resource:  "/a/b/c",
				Privilege: auth.WritePrivilege,
			},
			authorized: true,
			err:        nil,
		},
		{
			username: "slim",
			privileges: map[string][]auth.Privilege{
				"/": []auth.Privilege{auth.WritePrivilege, auth.ReadPrivilege, auth.DeletePrivilege},
			},
			action: auth.Action{
				Resource:  "/a/b/c",
				Privilege: auth.DeletePrivilege,
			},
			authorized: true,
			err:        nil,
		},
		{
			username: "joel",
			privileges: map[string][]auth.Privilege{
				"/": []auth.Privilege{auth.WritePrivilege, auth.ReadPrivilege},
			},
			action: auth.Action{
				Resource:  "/a/b/c",
				Privilege: auth.DeletePrivilege,
			},
			authorized: false,
			err:        errors.New(`user joel does not have "delete" privilege for resource "/a/b/c"`),
		},
		{
			username: "sky",
			privileges: map[string][]auth.Privilege{
				"/": []auth.Privilege{auth.ReadPrivilege, auth.DeletePrivilege},
			},
			action: auth.Action{
				Resource:  "/a/b/c",
				Privilege: auth.DeletePrivilege,
			},
			authorized: true,
			err:        nil,
		},
		{
			username: "alex",
			privileges: map[string][]auth.Privilege{
				"/": []auth.Privilege{auth.AllPrivileges},
			},
			action: auth.Action{
				Resource:  "/a/b/c",
				Privilege: auth.DeletePrivilege,
			},
			authorized: true,
			err:        nil,
		},
	}
	for _, tc := range testCases {
		u := auth.NewUser(tc.username, nil, tc.admin, tc.privileges)
		err := u.AuthorizeAction(tc.action)
		if err != nil {
			if tc.err == nil {
				t.Errorf("%s: unexpected error authorizing action: got %q", tc.username, err.Error())
			} else if err.Error() != tc.err.Error() {
				t.Errorf("%s: unexpected error message: got %q exp %q", tc.username, err.Error(), tc.err.Error())
			}
		} else {
			if tc.err != nil {
				t.Errorf("%s: AUTH BREACH: expected error authorizing action: %q", tc.username, tc.err.Error())
			}
		}
	}
}
