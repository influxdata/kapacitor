package kafka

import "testing"

func TestSASLAuth_Validate(t *testing.T) {
	tests := []struct {
		name    string
		auth    SASLAuth
		wantErr bool
	}{
		{
			name:    "Ignore empty SASL mechanism",
			auth:    SASLAuth{SASLMechanism: ""},
			wantErr: false,
		},
		{
			name:    "Invalid SASL mechanism",
			auth:    SASLAuth{SASLMechanism: "mech"},
			wantErr: true,
		},
		{
			name:    "Missing client ID",
			auth:    SASLAuth{SASLMechanism: "OAUTHBEARER", SASLOAUTHClientID: "", SASLOAUTHClientSecret: "secret"},
			wantErr: true,
		},
		{
			name:    "Missing client secret",
			auth:    SASLAuth{SASLMechanism: "OAUTHBEARER", SASLOAUTHClientID: "id", SASLOAUTHClientSecret: ""},
			wantErr: true,
		},
		{
			name:    "Invalid service",
			auth:    SASLAuth{SASLMechanism: "OAUTHBEARER", SASLOAUTHClientID: "id", SASLOAUTHClientSecret: "secret", SASLOAUTHService: "auth"},
			wantErr: true,
		},
		{
			name:    "Missing token url custom",
			auth:    SASLAuth{SASLMechanism: "OAUTHBEARER", SASLOAUTHClientID: "id", SASLOAUTHClientSecret: "secret", SASLOAUTHService: "custom", SASLOAUTHTokenURL: ""},
			wantErr: true,
		},
		{
			name:    "Missing token url empty (custom)",
			auth:    SASLAuth{SASLMechanism: "OAUTHBEARER", SASLOAUTHClientID: "id", SASLOAUTHClientSecret: "secret", SASLOAUTHService: "", SASLOAUTHTokenURL: ""},
			wantErr: true,
		},
		{
			name:    "Ok custom",
			auth:    SASLAuth{SASLMechanism: "OAUTHBEARER", SASLOAUTHClientID: "id", SASLOAUTHClientSecret: "secret", SASLOAUTHService: "custom", SASLOAUTHTokenURL: "url"},
			wantErr: false,
		},
		{
			name:    "Ok custom (empty)",
			auth:    SASLAuth{SASLMechanism: "OAUTHBEARER", SASLOAUTHClientID: "id", SASLOAUTHClientSecret: "secret", SASLOAUTHService: "", SASLOAUTHTokenURL: "url"},
			wantErr: false,
		},
		{
			name:    "Missing token url",
			auth:    SASLAuth{SASLMechanism: "OAUTHBEARER", SASLOAUTHClientID: "id", SASLOAUTHClientSecret: "secret", SASLOAUTHService: "auth0", SASLOAUTHTokenURL: ""},
			wantErr: true,
		},
		{
			name:    "Missing auth0 audience",
			auth:    SASLAuth{SASLMechanism: "OAUTHBEARER", SASLOAUTHClientID: "id", SASLOAUTHClientSecret: "secret", SASLOAUTHService: "auth0", SASLOAUTHTokenURL: "url"},
			wantErr: true,
		},
		{
			name:    "Ok auth0 ",
			auth:    SASLAuth{SASLMechanism: "OAUTHBEARER", SASLOAUTHClientID: "id", SASLOAUTHClientSecret: "secret", SASLOAUTHService: "auth0", SASLOAUTHTokenURL: "url", SASLOAUTHParams: map[string]string{"audience": "aud"}},
			wantErr: false,
		},
		{
			name:    "Azure OK",
			auth:    SASLAuth{SASLMechanism: "OAUTHBEARER", SASLOAUTHClientID: "id", SASLOAUTHClientSecret: "secret", SASLOAUTHService: "azuread", SASLOAUTHTenant: "tenant"},
			wantErr: false,
		},
		{
			name:    "Azure missing tenant",
			auth:    SASLAuth{SASLMechanism: "OAUTHBEARER", SASLOAUTHClientID: "id", SASLOAUTHClientSecret: "secret", SASLOAUTHService: "azuread"},
			wantErr: true,
		},
		{
			name:    "Azure Redundant token url",
			auth:    SASLAuth{SASLMechanism: "OAUTHBEARER", SASLOAUTHClientID: "id", SASLOAUTHClientSecret: "secret", SASLOAUTHService: "azuread", SASLOAUTHTokenURL: "url"},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.auth.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("SASLAuth.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
