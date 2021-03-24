package meta

import (
	"errors"
	"fmt"
)

var (
	// ErrAddressMissing is returned when a required addr parameter is
	// missing.
	ErrAddressMissing = errors.New("addr value is empty")

	// ErrHTTPAddressMissing is returned when a required httpAddr
	// parameter is missing.
	ErrHTTPAddressMissing = errors.New("httpAddr value is empty")

	// ErrTCPAddressMissing is returned when a required tcpAddr
	// parameter is missing.
	ErrTCPAddressMissing = errors.New("tcpAddr value is empty")

	// ErrLeaseNameMissing is returned when a required lease name is
	// missing.
	ErrLeaseNameMissing = errors.New("lease name is empty")

	// ErrLeaseConflict is returned when there is a conflict while
	// attempting to require a lease.
	ErrLeaseConflict = errors.New("another node owns the lease")

	// ErrMaximumRedirectsReached is returned when a Client has been
	// redirected too many times for a single request.
	ErrMaximumRedirectsReached = errors.New("too many redirects")

	// ErrNotMetaNode is returned when attempting to add a non-meta node
	// as a meta node in the cluster.
	ErrNotMetaNode = errors.New("not a meta node")

	// ErrEmptyCluster is returned when there are no nodes known to the
	// cluster.
	ErrEmptyCluster = errors.New("cluster is empty")

	ErrEmptyStatus = errors.New("status is empty")

	// ErrAntiEntropyDisabled is returned when the anti-entropy service
	// returns a 501 Not Implemented HTTP status.
	ErrAntiEntropyDisabled = errors.New("anti-entropy service is not enabled")
)

// A TimeoutError is returned when an operation on the meta node API was
// not successful, and repeated attempts to succeed have failed.
//
// TimeoutError makes the last underlying error available in the
// Context field.
type TimeoutError struct {
	Context error
}

func (e TimeoutError) Error() string {
	return fmt.Sprintf("operation timed out with error: %v", e.Context)
}

// ErrTimeout returns a new TimeoutError.
func ErrTimeout(ctx error) TimeoutError {
	return TimeoutError{Context: ctx}
}

// A FatalError is returned when an operation on the meta node API was
// not successful and retrying the operation would always result in the
// same error being returned.
//
// FatalError makes the last underlying error available in the
// Context field.
type FatalError struct {
	Context error
}

func (e FatalError) Error() string {
	return fmt.Sprintf("operation exited with error: %v", e.Context)
}
