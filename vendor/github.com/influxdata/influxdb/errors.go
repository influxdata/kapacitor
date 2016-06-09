package influxdb

import (
	"errors"
	"fmt"
	"strings"
)

var (
	// ErrFieldsRequired is returned when a point does not any fields.
	ErrFieldsRequired = errors.New("fields required")

	// ErrFieldTypeConflict is returned when a new field already exists with a different type.
	ErrFieldTypeConflict = errors.New("field type conflict")

	// ErrUpgradeEngine will be returned when it's determined that
	// the server has encountered shards that are not in the `tsm1`
	// format.
	ErrUpgradeEngine = errors.New("\n\n" + upgradeMessage + "\n\n")
)

// ErrDatabaseNotFound indicates that a database operation failed on the
// specified database because the specified database does not exist.
func ErrDatabaseNotFound(name string) error { return fmt.Errorf("database not found: %s", name) }

// ErrRetentionPolicyNotFound indicates that the named retention policy could
// not be found in the database.
func ErrRetentionPolicyNotFound(name string) error {
	return fmt.Errorf("retention policy not found: %s", name)
}

// IsClientError indicates whether an error is a known client error.
func IsClientError(err error) bool {
	if err == nil {
		return false
	}

	if err == ErrFieldsRequired {
		return true
	}
	if err == ErrFieldTypeConflict {
		return true
	}

	if strings.Contains(err.Error(), ErrFieldTypeConflict.Error()) {
		return true
	}

	return false
}

const upgradeMessage = `*******************************************************************
                 UNSUPPORTED SHARD FORMAT DETECTED

As of version 0.11, only tsm shards are supported. Please use the
influx_tsm tool to convert non-tsm shards.

More information can be found at the documentation site:
https://docs.influxdata.com/influxdb/v0.10/administration/upgrading
*******************************************************************`
