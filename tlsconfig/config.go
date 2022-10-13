package tlsconfig

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
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
		caCert, err := os.ReadFile(SSLCA)
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
	if out == nil {
		out = new(tls.Config)
	}

	if len(c.Ciphers) > 0 {
		for _, name := range c.Ciphers {
			strUpperName := strings.ToUpper(name)
			cipher, ok := ciphers[strUpperName]
			if !ok {
				return nil, badCipher(name)
			}
			out.CipherSuites = append(out.CipherSuites, cipher)
		}
	} else {
		for _, cipher := range ciphers {
			out.CipherSuites = append(out.CipherSuites, cipher)
		}
	}

	if c.MinVersion != "" {
		version, ok := versionsMap[strings.ToUpper(c.MinVersion)]
		if !ok {
			return nil, badVersion(c.MinVersion)
		}
		out.MinVersion = version
	} else {
		out.MinVersion = tls.VersionTLS12
	}

	if c.MaxVersion != "" {
		version, ok := versionsMap[strings.ToUpper(c.MaxVersion)]
		if !ok {
			return nil, badVersion(c.MaxVersion)
		}
		out.MaxVersion = version
	}

	return out, nil
}

var ciphers = map[string]uint16{
	"TLS_RSA_WITH_AES_128_CBC_SHA":            tls.TLS_RSA_WITH_AES_128_CBC_SHA,
	"TLS_RSA_WITH_AES_256_CBC_SHA":            tls.TLS_RSA_WITH_AES_256_CBC_SHA,
	"TLS_RSA_WITH_AES_128_CBC_SHA256":         tls.TLS_RSA_WITH_AES_128_CBC_SHA256,
	"TLS_RSA_WITH_AES_128_GCM_SHA256":         tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
	"TLS_RSA_WITH_AES_256_GCM_SHA384":         tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
	"TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA":    tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,
	"TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA":    tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,
	"TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA":      tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
	"TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA":      tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
	"TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256":   tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256,
	"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256":   tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
	"TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256": tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
	"TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384":   tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
	"TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384": tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
	"TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305":    tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
	"TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305":  tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,

	// TLS1.3 cypher suites
	"TLS_AES_128_GCM_SHA256":       tls.TLS_AES_128_GCM_SHA256,
	"TLS_AES_256_GCM_SHA384":       tls.TLS_AES_256_GCM_SHA384,
	"TLS_CHACHA20_POLY1305_SHA256": tls.TLS_CHACHA20_POLY1305_SHA256,
}

var availableCiphers = func() string {
	available := make([]string, 0, len(versionsMap))
	for name := range ciphers {
		if name[0] == '1' {
			continue
		}
		available = append(available, name)
	}
	sort.Strings(available)
	return strings.Join(available, ", ")
}()

// we do not use these because they are insecure
var deprecatedCiphers = map[string]struct{}{
	"TLS_RSA_WITH_3DES_EDE_CBC_SHA":       struct{}{}, // broken by sweet32 https://sweet32.info/
	"TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA": struct{}{}, // broken by sweet32 https://sweet32.info/
	"TLS_ECDHE_RSA_WITH_RC4_128_SHA":      struct{}{}, // Broken cipher RC4 is deprecated by RFC 7465
	"TLS_ECDHE_ECDSA_WITH_RC4_128_SHA":    struct{}{}, // Broken cipher RC4 is deprecated by RFC 7465
	"TLS_RSA_WITH_RC4_128_SHA":            struct{}{}, // Broken cipher RC4 is deprecated by RFC 7465

}

func badCipher(name string) error {
	if _, ok := deprecatedCiphers[name]; ok {
		return fmt.Errorf("deprecated cipher suite: %q. available versions: %s", name, availableCiphers)
	}
	return fmt.Errorf("unknown cipher suite: %q. available ciphers: %s", name, availableCiphers)
}

var versionsMap = map[string]uint16{
	"TLS1.1": tls.VersionTLS11,
	"1.1":    tls.VersionTLS11,
	"TLS1.2": tls.VersionTLS12,
	"1.2":    tls.VersionTLS12,
	"TLS1.3": tls.VersionTLS13,
	"1.3":    tls.VersionTLS13,
}

var availableVersions = func() string {
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
	return strings.Join(available, ", ")
}()

var deprecatedVersions = map[string]struct{}{
	"SSL3.0": struct{}{},
	"TLS1.0": struct{}{},
	"1.0":    struct{}{},
	"TLS1.1": struct{}{},
	"1.1":    struct{}{},
}

func badVersion(name string) error {
	if _, ok := deprecatedVersions[name]; ok {
		return fmt.Errorf("deprecated tls version: %q. available versions: %s", name, availableVersions)
	}
	return fmt.Errorf("unknown tls version: %q. available versions: %s", name, availableVersions)
}
