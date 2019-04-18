package tlsconfig

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"sort"
	"strings"
)

// Create creates a new tls.Config object from the given certs, key, and CA files.
func Create(
	SSLCA, SSLCert, SSLKey string,
	InsecureSkipVerify bool,
) (*tls.Config, error) {
	t := &tls.Config{
		InsecureSkipVerify: InsecureSkipVerify,
	}
	if SSLCert != "" && SSLKey != "" {
		cert, err := tls.LoadX509KeyPair(SSLCert, SSLKey)
		if err != nil {
			return nil, fmt.Errorf(
				"Could not load TLS client key/certificate: %s",
				err)
		}
		t.Certificates = []tls.Certificate{cert}
	} else if SSLCert != "" {
		return nil, errors.New("Must provide both key and cert files: only cert file provided.")
	} else if SSLKey != "" {
		return nil, errors.New("Must provide both key and cert files: only key file provided.")
	}

	if SSLCA != "" {
		caCert, err := ioutil.ReadFile(SSLCA)
		if err != nil {
			return nil, fmt.Errorf("Could not load TLS CA: %s",
				err)
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		t.RootCAs = caCertPool
	}
	return t, nil
}

type Config struct {
	Ciphers    []string `toml:"ciphers"`
	MinVersion string   `toml:"min-version"`
	MaxVersion string   `toml:"max-version"`
}

func NewConfig() Config {
	return Config{}
}

func (c Config) Validate() error {
	_, err := c.Parse()
	return err
}

func (c Config) Parse() (out *tls.Config, err error) {
	if len(c.Ciphers) > 0 {
		if out == nil {
			out = new(tls.Config)
		}

		for _, name := range c.Ciphers {
			cipher, ok := ciphersMap[strings.ToUpper(name)]
			if !ok {
				return nil, unknownCipher(name)
			}
			out.CipherSuites = append(out.CipherSuites, cipher)
		}
	}

	if c.MinVersion != "" {
		if out == nil {
			out = new(tls.Config)
		}

		version, ok := versionsMap[strings.ToUpper(c.MinVersion)]
		if !ok {
			return nil, unknownVersion(c.MinVersion)
		}
		out.MinVersion = version
	}

	if c.MaxVersion != "" {
		if out == nil {
			out = new(tls.Config)
		}

		version, ok := versionsMap[strings.ToUpper(c.MaxVersion)]
		if !ok {
			return nil, unknownVersion(c.MaxVersion)
		}
		out.MaxVersion = version
	}

	return out, nil
}

var ciphersMap = map[string]uint16{
	"TLS_RSA_WITH_RC4_128_SHA":                tls.TLS_RSA_WITH_RC4_128_SHA,
	"TLS_RSA_WITH_3DES_EDE_CBC_SHA":           tls.TLS_RSA_WITH_3DES_EDE_CBC_SHA,
	"TLS_RSA_WITH_AES_128_CBC_SHA":            tls.TLS_RSA_WITH_AES_128_CBC_SHA,
	"TLS_RSA_WITH_AES_256_CBC_SHA":            tls.TLS_RSA_WITH_AES_256_CBC_SHA,
	"TLS_RSA_WITH_AES_128_CBC_SHA256":         tls.TLS_RSA_WITH_AES_128_CBC_SHA256,
	"TLS_RSA_WITH_AES_128_GCM_SHA256":         tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
	"TLS_RSA_WITH_AES_256_GCM_SHA384":         tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
	"TLS_ECDHE_ECDSA_WITH_RC4_128_SHA":        tls.TLS_ECDHE_ECDSA_WITH_RC4_128_SHA,
	"TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA":    tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,
	"TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA":    tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,
	"TLS_ECDHE_RSA_WITH_RC4_128_SHA":          tls.TLS_ECDHE_RSA_WITH_RC4_128_SHA,
	"TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA":     tls.TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA,
	"TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA":      tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
	"TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA":      tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
	"TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256": tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256,
	"TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256":   tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256,
	"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256":   tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
	"TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256": tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
	"TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384":   tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
	"TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384": tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
	"TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305":    tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
	"TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305":  tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
}

func unknownCipher(name string) error {
	available := make([]string, 0, len(ciphersMap))
	for name := range ciphersMap {
		available = append(available, name)
	}
	sort.Strings(available)

	return fmt.Errorf("unknown cipher suite: %q. available ciphers: %s",
		name, strings.Join(available, ", "))
}

var versionsMap = map[string]uint16{
	"SSL3.0": tls.VersionSSL30,
	"TLS1.0": tls.VersionTLS10,
	"1.0":    tls.VersionTLS10,
	"TLS1.1": tls.VersionTLS11,
	"1.1":    tls.VersionTLS11,
	"TLS1.2": tls.VersionTLS12,
	"1.2":    tls.VersionTLS12,
}

func unknownVersion(name string) error {
	available := make([]string, 0, len(versionsMap))
	for name := range versionsMap {
		// skip the ones that just begin with a number. they may be confusing
		// due to the duplication, and just help if the user specifies without
		// the TLS part.
		if name[0] == '1' {
			continue
		}
		available = append(available, name)
	}
	sort.Strings(available)

	return fmt.Errorf("unknown tls version: %q. available versions: %s",
		name, strings.Join(available, ", "))
}
