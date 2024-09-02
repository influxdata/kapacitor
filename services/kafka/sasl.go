package kafka

import (
	"context"
	"errors"
	"net/url"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"

	"github.com/IBM/sarama"
)

type SASLAuth struct {
	SASLUsername   string            `toml:"sasl-username" override:"sasl-username"`
	SASLPassword   string            `toml:"sasl-password" override:"sasl-password"`
	SASLExtensions map[string]string `toml:"sasl_extensions" override:"sasl_extensions"`
	SASLMechanism  string            `toml:"sasl-mechanism" override:"sasl-mechanism"`
	SASLVersion    *int              `toml:"sasl-version" override:"sasl-version"`

	// GSSAPI config
	SASLGSSAPIServiceName        string `toml:"sasl-gssapi-service-name" override:"sasl-gssapi-service-name"`
	SASLGSSAPIAuthType           string `toml:"sasl-gssapi-auth-type" override:"sasl-gssapi-auth-type"`
	SASLGSSAPIDisablePAFXFAST    bool   `toml:"sasl-gssapi-disable-pafxfast" override:"sasl-gssapi-disable-pafxfast"`
	SASLGSSAPIKerberosConfigPath string `toml:"sasl-gssapi-kerberos-config-path" override:"sasl-gssapi-kerberos-config-path"`
	SASLGSSAPIKeyTabPath         string `toml:"sasl-gssapi-key-tab-path" override:"sasl-gssapi-key-tab-path"`
	SASLGSSAPIRealm              string `toml:"sasl-gssapi-realm" override:"sasl-gssapi-realm"`

	// OAUTHBEARER config
	SASLOAUTHService      string            `toml:"sasl-oauth-service" override:"sasl-oauth-service"`
	SASLOAUTHClientID     string            `toml:"sasl-oauth-client-id" override:"sasl-oauth-client-id"`
	SASLOAUTHClientSecret string            `toml:"sasl-oauth-client-secret" override:"sasl-oauth-client-secret"`
	SASLOAUTHTokenURL     string            `toml:"sasl-oauth-token-url" override:"sasl-oauth-token-url"`
	SASLOAUTHScopes       []string          `toml:"sasl-oauth-scopes" override:"sasl-oauth-scopes"`
	SASLOAUTHParams       map[string]string `toml:"sasl-oauth-parameters" override:"sasl-oauth-parameters"`
	SASLOAUTHExpiryMargin time.Duration     `toml:"sasl-oauth-token-expiry-margin" override:"sasl-oauth-token-expiry-margin"`
	//Deprecated, not used
	SASLAccessToken string `toml:"sasl-access-token" override:"sasl-access-token"`

	source oauth2.TokenSource
	cancel context.CancelFunc
}

// SetSASLConfig configures SASL for kafka (sarama)
// We mutate instead of returning the appropriate struct, because sarama.NewConfig() already populates certain defaults
// that we do not want to disrupt.
func (k *SASLAuth) SetSASLConfig(config *sarama.Config) error {
	ctx, cancel := context.WithCancel(context.Background())
	k.cancel = cancel

	config.Net.SASL.User = k.SASLUsername
	config.Net.SASL.Password = k.SASLPassword

	if k.SASLMechanism != "" {
		config.Net.SASL.Mechanism = sarama.SASLMechanism(k.SASLMechanism)
		switch config.Net.SASL.Mechanism {
		case sarama.SASLTypeSCRAMSHA256:
			config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
				return &XDGSCRAMClient{HashGeneratorFcn: SHA256}
			}
		case sarama.SASLTypeSCRAMSHA512:
			config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
				return &XDGSCRAMClient{HashGeneratorFcn: SHA512}
			}
		case sarama.SASLTypeOAuth:
			config.Net.SASL.TokenProvider = k // use self as token provider.
			var endpoint oauth2.Endpoint
			if k.SASLOAUTHService == "auth0" {
				endpoint = oauth2.Endpoint{
					TokenURL:  k.SASLOAUTHTokenURL,
					AuthStyle: oauth2.AuthStyleInParams,
				}
			}
			if k.SASLOAUTHExpiryMargin == 0 {
				k.SASLOAUTHExpiryMargin = 1 * time.Second
			}
			cfg := &clientcredentials.Config{
				ClientID:       k.SASLOAUTHClientID,
				ClientSecret:   k.SASLOAUTHClientSecret,
				TokenURL:       endpoint.TokenURL,
				Scopes:         k.SASLOAUTHScopes,
				AuthStyle:      endpoint.AuthStyle,
				EndpointParams: url.Values{},
			}
			for k, v := range k.SASLOAUTHParams {
				cfg.EndpointParams.Add(k, v)
			}
			src := cfg.TokenSource(ctx)
			k.source = oauth2.ReuseTokenSourceWithExpiry(nil, src, time.Duration(k.SASLOAUTHExpiryMargin))

		case sarama.SASLTypeGSSAPI:
			config.Net.SASL.GSSAPI.ServiceName = k.SASLGSSAPIServiceName
			config.Net.SASL.GSSAPI.AuthType = gssapiAuthType(k.SASLGSSAPIAuthType)
			config.Net.SASL.GSSAPI.Username = k.SASLUsername
			config.Net.SASL.GSSAPI.Password = k.SASLPassword
			config.Net.SASL.GSSAPI.DisablePAFXFAST = k.SASLGSSAPIDisablePAFXFAST
			config.Net.SASL.GSSAPI.KerberosConfigPath = k.SASLGSSAPIKerberosConfigPath
			config.Net.SASL.GSSAPI.KeyTabPath = k.SASLGSSAPIKeyTabPath
			config.Net.SASL.GSSAPI.Realm = k.SASLGSSAPIRealm

		case sarama.SASLTypePlaintext:
			// nothing.
		default:
		}
	}

	if k.SASLUsername != "" || k.SASLMechanism != "" {
		config.Net.SASL.Enable = true

		version, err := SASLVersion(config.Version, k.SASLVersion)
		if err != nil {
			return err
		}
		config.Net.SASL.Version = version
	}
	return nil
}

// Token does nothing smart, it just grabs a hard-coded token from config.
func (k *SASLAuth) Token() (*sarama.AccessToken, error) {
	token, err := k.source.Token()
	if err != nil {
		return nil, err
	}
	return &sarama.AccessToken{
		Token:      token.AccessToken,
		Extensions: k.SASLExtensions,
	}, nil
}

func (k *SASLAuth) Equals(other *SASLAuth) bool {
	if k.SASLUsername != other.SASLUsername {
		return false
	}
	if k.SASLPassword != other.SASLPassword {
		return false
	}
	if k.SASLMechanism != other.SASLMechanism {
		return false
	}
	if k.SASLVersion != other.SASLVersion {
		return false
	}
	if k.SASLGSSAPIServiceName != other.SASLGSSAPIServiceName {
		return false
	}
	if k.SASLGSSAPIAuthType != other.SASLGSSAPIAuthType {
		return false
	}
	if k.SASLGSSAPIDisablePAFXFAST != other.SASLGSSAPIDisablePAFXFAST {
		return false
	}
	if k.SASLGSSAPIKerberosConfigPath != other.SASLGSSAPIKerberosConfigPath {
		return false
	}
	if k.SASLGSSAPIKeyTabPath != other.SASLGSSAPIKeyTabPath {
		return false
	}
	if k.SASLGSSAPIRealm != other.SASLGSSAPIRealm {
		return false
	}
	if k.SASLOAUTHService != other.SASLOAUTHService {
		return false
	}
	if k.SASLOAUTHClientID != other.SASLOAUTHClientID {
		return false
	}
	if k.SASLOAUTHClientSecret != other.SASLOAUTHClientSecret {
		return false
	}
	if k.SASLOAUTHTokenURL != other.SASLOAUTHTokenURL {
		return false
	}
	if len(k.SASLOAUTHScopes) != len(other.SASLOAUTHScopes) {
		return false
	}
	for i, v := range k.SASLOAUTHScopes {
		if v != other.SASLOAUTHScopes[i] {
			return false
		}
	}
	if len(k.SASLOAUTHParams) != len(other.SASLOAUTHParams) {
		return false
	}
	for k, v := range k.SASLOAUTHParams {
		if v != other.SASLOAUTHParams[k] {
			return false
		}
	}
	if k.SASLOAUTHExpiryMargin != other.SASLOAUTHExpiryMargin {
		return false
	}
	return true
}

func SASLVersion(kafkaVersion sarama.KafkaVersion, saslVersion *int) (int16, error) {
	if saslVersion == nil {
		if kafkaVersion.IsAtLeast(sarama.V1_0_0_0) {
			return sarama.SASLHandshakeV1, nil
		}
		return sarama.SASLHandshakeV0, nil
	}

	switch *saslVersion {
	case 0:
		return sarama.SASLHandshakeV0, nil
	case 1:
		return sarama.SASLHandshakeV1, nil
	default:
		return 0, errors.New("invalid SASL version")
	}
}

func gssapiAuthType(authType string) int {
	switch authType {
	case "KRB5_USER_AUTH":
		return sarama.KRB5_USER_AUTH
	case "KRB5_KEYTAB_AUTH":
		return sarama.KRB5_KEYTAB_AUTH
	default:
		return 0
	}
}
