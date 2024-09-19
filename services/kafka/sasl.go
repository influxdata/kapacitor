package kafka

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"time"

	"golang.org/x/oauth2/endpoints"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"

	kafka "github.com/IBM/sarama"
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
	// Service name for OAuth2 token endpoint: empty or custom, auth0, azuread
	SASLOAUTHService      string            `toml:"sasl-oauth-service" override:"sasl-oauth-service"`
	SASLOAUTHClientID     string            `toml:"sasl-oauth-client-id" override:"sasl-oauth-client-id"`
	SASLOAUTHClientSecret string            `toml:"sasl-oauth-client-secret" override:"sasl-oauth-client-secret"`
	SASLOAUTHTokenURL     string            `toml:"sasl-oauth-token-url" override:"sasl-oauth-token-url"`
	SASLOAUTHScopes       []string          `toml:"sasl-oauth-scopes" override:"sasl-oauth-scopes"`
	SASLOAUTHParams       map[string]string `toml:"sasl-oauth-parameters" override:"sasl-oauth-parameters"`
	SASLOAUTHExpiryMargin time.Duration     `toml:"sasl-oauth-token-expiry-margin" override:"sasl-oauth-token-expiry-margin"`
	// Static token, if set it will override the token source.
	SASLAccessToken string `toml:"sasl-access-token" override:"sasl-access-token"`
	// Tenant ID for AzureAD
	SASLOAUTHTenant string `toml:"sasl-oauth-tenant-id" override:"sasl-oauth-tenant-id"`
}

func (k *SASLAuth) Validate() error {
	switch k.SASLMechanism {
	case "", kafka.SASLTypeSCRAMSHA256, kafka.SASLTypeSCRAMSHA512, kafka.SASLTypeGSSAPI, kafka.SASLTypePlaintext:
		return nil
	case kafka.SASLTypeOAuth:
		if k.SASLAccessToken != "" && (k.SASLOAUTHService != "" || k.SASLOAUTHTokenURL != "") {
			return errors.New("cannot set 'sasl-access-token' with 'sasl-oauth-service' and 'sasl-oauth-token-url'")
		}
		if k.SASLOAUTHClientID == "" || k.SASLOAUTHClientSecret == "" {
			return errors.New("'sasl-oauth-client-id' and 'sasl-oauth-client-secret' are required")
		}
		service := strings.ToLower(k.SASLOAUTHService)
		switch service {
		case "", "custom":
			if k.SASLOAUTHTokenURL == "" {
				return errors.New("'sasl-oauth-token-url' required for custom service")
			}
		case "auth0":
			if k.SASLOAUTHTokenURL == "" {
				return errors.New("'sasl-oauth-token-url' required for Auth0")
			}
			if audience := k.SASLOAUTHParams["audience"]; audience == "" {
				return errors.New("'audience' parameter is required for Auth0")
			}
		case "azuread":
			if k.SASLOAUTHTenant == "" {
				return errors.New("'sasl-oauth-tenant-id' required for AzureAD")
			}
			if k.SASLOAUTHTokenURL != "" {
				return errors.New("'sasl-oauth-token-url' cannot be set for service " + k.SASLOAUTHService)
			}
		default:
			return errors.New("service " + k.SASLOAUTHService + " not supported")
		}
	default:
		return errors.New("invalid sasl-mechanism")
	}
	return nil
}

// SetSASLConfig configures SASL for kafka (sarama)
// We mutate instead of returning the appropriate struct, because kafka.NewConfig() already populates certain defaults
// that we do not want to disrupt.
func (k *SASLAuth) SetSASLConfig(config *kafka.Config) error {

	config.Net.SASL.User = k.SASLUsername
	config.Net.SASL.Password = k.SASLPassword

	if k.SASLMechanism != "" {
		config.Net.SASL.Mechanism = kafka.SASLMechanism(k.SASLMechanism)
		switch config.Net.SASL.Mechanism {
		case kafka.SASLTypeSCRAMSHA256:
			config.Net.SASL.SCRAMClientGeneratorFunc = func() kafka.SCRAMClient {
				return &XDGSCRAMClient{HashGeneratorFcn: SHA256}
			}
		case kafka.SASLTypeSCRAMSHA512:
			config.Net.SASL.SCRAMClientGeneratorFunc = func() kafka.SCRAMClient {
				return &XDGSCRAMClient{HashGeneratorFcn: SHA512}
			}
		case kafka.SASLTypeOAuth:
			if k.SASLAccessToken != "" {
				config.Net.SASL.TokenProvider = NewStaticToken(k.SASLAccessToken, k.SASLExtensions)
				break
			}
			var endpoint oauth2.Endpoint
			service := strings.ToLower(k.SASLOAUTHService)
			switch service {
			case "", "custom":
				endpoint = oauth2.Endpoint{
					TokenURL:  k.SASLOAUTHTokenURL,
					AuthStyle: oauth2.AuthStyleAutoDetect,
				}
			case "auth0":
				endpoint = oauth2.Endpoint{
					TokenURL:  k.SASLOAUTHTokenURL,
					AuthStyle: oauth2.AuthStyleInParams,
				}
			case "azuread":
				endpoint = endpoints.AzureAD(k.SASLOAUTHTenant)
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
			ctx, cancel := context.WithCancel(context.Background())
			src := cfg.TokenSource(ctx)
			source := oauth2.ReuseTokenSourceWithExpiry(nil, src, k.SASLOAUTHExpiryMargin)
			config.Net.SASL.TokenProvider = NewRefreshingToken(source, cancel, k.SASLExtensions)

		case kafka.SASLTypeGSSAPI:
			config.Net.SASL.GSSAPI.ServiceName = k.SASLGSSAPIServiceName
			config.Net.SASL.GSSAPI.AuthType = gssapiAuthType(k.SASLGSSAPIAuthType)
			config.Net.SASL.GSSAPI.Username = k.SASLUsername
			config.Net.SASL.GSSAPI.Password = k.SASLPassword
			config.Net.SASL.GSSAPI.DisablePAFXFAST = k.SASLGSSAPIDisablePAFXFAST
			config.Net.SASL.GSSAPI.KerberosConfigPath = k.SASLGSSAPIKerberosConfigPath
			config.Net.SASL.GSSAPI.KeyTabPath = k.SASLGSSAPIKeyTabPath
			config.Net.SASL.GSSAPI.Realm = k.SASLGSSAPIRealm
		case kafka.SASLTypePlaintext:
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

func SASLVersion(kafkaVersion kafka.KafkaVersion, saslVersion *int) (int16, error) {
	if saslVersion == nil {
		if kafkaVersion.IsAtLeast(kafka.V1_0_0_0) {
			return kafka.SASLHandshakeV1, nil
		}
		return kafka.SASLHandshakeV0, nil
	}

	switch *saslVersion {
	case 0:
		return kafka.SASLHandshakeV0, nil
	case 1:
		return kafka.SASLHandshakeV1, nil
	default:
		return 0, errors.New("invalid SASL version")
	}
}

func gssapiAuthType(authType string) int {
	switch authType {
	case "KRB5_USER_AUTH":
		return kafka.KRB5_USER_AUTH
	case "KRB5_KEYTAB_AUTH":
		return kafka.KRB5_KEYTAB_AUTH
	default:
		return 0
	}
}
