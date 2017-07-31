package edge

import (
	"errors"
)

// ErrAborted is returned from the Edge interface when operations are performed on the edge after it has been aborted.
var ErrAborted = errors.New("edge aborted")
