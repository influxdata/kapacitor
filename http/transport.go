package http

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"syscall"
	"time"

	furl "github.com/influxdata/flux/dependencies/url"
)

// NewDefaultTransport creates a new transport with sane defaults.
func NewDefaultTransport(dialer *net.Dialer) *http.Transport {

	if dialer == nil {
		dialer = &net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}
	}

	// These defaults are copied from http.DefaultTransport.
	return &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (dialer).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		// Below are changes from http.DefaultTransport
		MaxIdleConnsPerHost: 100, // increased from 2, services tend to connect to a single host
	}
}

// NewDefaultTransportWithTLS creates a new transport with the specified TLS configuration.
func NewDefaultTransportWithTLS(tlsConfig *tls.Config, dialer *net.Dialer) *http.Transport {
	t := NewDefaultTransport(dialer)
	t.TLSClientConfig = tlsConfig
	return t
}

// Control is called after DNS lookup, but before the network connection is
// initiated.
func Control(urlValidator furl.Validator) func(network, address string, c syscall.RawConn) error {
	return func(network, address string, c syscall.RawConn) error {
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return err
		}

		ip := net.ParseIP(host)
		return urlValidator.ValidateIP(ip)
	}
}

// NewDefaultClientWithTLS creates a tls client with sane defaults.
func NewDefaultClientWithTLS(tlsConfig *tls.Config, urlValidator furl.Validator) *http.Client {

	// These defaults are copied from http.DefaultTransport.
	return &http.Client{
		Transport: NewDefaultTransportWithTLS(tlsConfig, &net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			Control:   Control(urlValidator),
		}),
		Timeout: 30 * time.Second,
	}
}

// NewDefaultClient creates a client with sane defaults.
func NewDefaultClient(urlValidator furl.Validator) *http.Client {

	// These defaults are copied from http.DefaultTransport.
	return &http.Client{
		Transport: NewDefaultTransport(&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			Control:   Control(urlValidator),
			// DualStack is deprecated
		}),
	}
}

// DefaultValidator is the default validator, it can be replaced at start time with a different validator
var DefaultValidator furl.Validator = furl.PassValidator{}

type cidrValidator struct {
	cidrs []*net.IPNet
}

func (v cidrValidator) Validate(u *url.URL) error {
	ips, err := net.LookupIP(u.Hostname())
	if err != nil {
		return err
	}
	for _, ip := range ips {
		err = v.ValidateIP(ip)
		if err != nil {
			return err
		}
	}
	return nil
}

func (v cidrValidator) ValidateIP(ip net.IP) error {
	for i := range v.cidrs {
		if v.cidrs[i].Contains(ip) {
			return fmt.Errorf("ip '%s' is on deny list", ip)
		}
	}
	return nil
}

func ParseCIDRsString(s string) (furl.Validator, error) {
	cidrStrings := strings.Split(s, ",")
	cidrs := make([]*net.IPNet, 0, len(cidrStrings))
	for i := range cidrStrings {
		_, cidr, err := net.ParseCIDR(cidrStrings[i])
		if err != nil {
			return nil, err
		}
		cidrs = append(cidrs, cidr)
	}
	return cidrValidator{cidrs: cidrs}, nil
}
