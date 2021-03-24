package auth

import (
	"reflect"
	"testing"

	"github.com/influxdata/kapacitor/services/auth/meta"
)

func Test_convertPMPerissions(t *testing.T) {
	testCases := []struct {
		scopedPerms map[string][]meta.Permission
		exp         map[string][]Privilege
	}{
		{
			scopedPerms: map[string][]meta.Permission{
				"": {
					meta.KapacitorAPIPermission,
				},
			},
			exp: map[string][]Privilege{
				"/api":        {AllPrivileges},
				"/api/config": {NoPrivileges},
			},
		},
		{
			scopedPerms: map[string][]meta.Permission{
				"": {
					meta.KapacitorAPIPermission,
					meta.KapacitorConfigAPIPermission,
					meta.WriteDataPermission,
					meta.ReadDataPermission,
				},
			},
			exp: map[string][]Privilege{
				"/api/ping":   {AllPrivileges},
				"/api":        {AllPrivileges},
				"/api/config": {AllPrivileges},
				"/api/write":  {WritePrivilege},
				"/database":   {WritePrivilege, ReadPrivilege},
			},
		},
		{
			scopedPerms: map[string][]meta.Permission{
				"": {
					meta.KapacitorAPIPermission,
					meta.WriteDataPermission,
					meta.ReadDataPermission,
				},
			},
			exp: map[string][]Privilege{
				"/api/ping":   {AllPrivileges},
				"/api":        {AllPrivileges},
				"/api/config": {NoPrivileges},
				"/api/write":  {WritePrivilege},
				"/database":   {WritePrivilege, ReadPrivilege},
			},
		},
		{
			scopedPerms: map[string][]meta.Permission{
				"": {
					meta.KapacitorConfigAPIPermission,
				},
			},
			exp: map[string][]Privilege{
				"/api/ping":   {AllPrivileges},
				"/api/config": {AllPrivileges},
			},
		},
		{
			scopedPerms: map[string][]meta.Permission{
				"": {
					meta.WriteDataPermission,
					meta.ReadDataPermission,
				},
			},
			exp: map[string][]Privilege{
				"/api/ping":  {AllPrivileges},
				"/api/write": {WritePrivilege},
				"/database":  {WritePrivilege, ReadPrivilege},
			},
		},
		{
			scopedPerms: map[string][]meta.Permission{
				"": {
					meta.WriteDataPermission,
				},
			},
			exp: map[string][]Privilege{
				"/api/ping":  {AllPrivileges},
				"/api/write": {WritePrivilege},
				"/database":  {WritePrivilege},
			},
		},
		{
			scopedPerms: map[string][]meta.Permission{
				"": {
					meta.ReadDataPermission,
				},
			},
			exp: map[string][]Privilege{
				"/api/ping": {AllPrivileges},
				"/database": {ReadPrivilege},
			},
		},
		{
			scopedPerms: map[string][]meta.Permission{
				"mydb": {
					meta.WriteDataPermission,
					meta.ReadDataPermission,
				},
			},
			exp: map[string][]Privilege{
				"/api/ping":            {AllPrivileges},
				"/api/write":           {WritePrivilege},
				"/database/mydb_clean": {WritePrivilege, ReadPrivilege},
			},
		},
		{
			scopedPerms: map[string][]meta.Permission{
				"mydb": {
					meta.WriteDataPermission,
				},
			},
			exp: map[string][]Privilege{
				"/api/ping":            {AllPrivileges},
				"/api/write":           {WritePrivilege},
				"/database/mydb_clean": {WritePrivilege},
			},
		},
		{
			scopedPerms: map[string][]meta.Permission{
				"mydb": {
					meta.ReadDataPermission,
				},
			},
			exp: map[string][]Privilege{
				"/api/ping":            {AllPrivileges},
				"/database/mydb_clean": {ReadPrivilege},
			},
		},
		{
			scopedPerms: map[string][]meta.Permission{
				"mydb": {
					meta.WriteDataPermission,
				},
				"otherdb": {
					meta.ReadDataPermission,
				},
			},
			exp: map[string][]Privilege{
				"/api/ping":               {AllPrivileges},
				"/api/write":              {WritePrivilege},
				"/database/mydb_clean":    {WritePrivilege},
				"/database/otherdb_clean": {ReadPrivilege},
			},
		},
		{
			scopedPerms: map[string][]meta.Permission{
				"mydb/slash/in/name": {
					meta.WriteDataPermission,
				},
				"otherdb/moreslashes": {
					meta.ReadDataPermission,
				},
			},
			exp: map[string][]Privilege{
				"/api/ping":                           {AllPrivileges},
				"/api/write":                          {WritePrivilege},
				"/database/mydb_slash_in_name_dirty":  {WritePrivilege},
				"/database/otherdb_moreslashes_dirty": {ReadPrivilege},
			},
		},
	}

	for _, tc := range testCases {
		if got := convertPMPermissions(tc.scopedPerms); !reflect.DeepEqual(got, tc.exp) {
			t.Errorf("unexpected converted privileges:\npermisions:\n%v\ngot\n%v\nexp\n%v\n", tc.scopedPerms, got, tc.exp)
		}
	}
}
