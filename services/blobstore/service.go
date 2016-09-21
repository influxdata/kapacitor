// Blobstore provides an interface for storing, retrieving and tagging immutable blobs of data.
package blobstore

import (
	"io"
	"time"
)

// Interface for storing/retrieving and tagging blobs.
type Interface interface {
	// Put a blob into the store.
	// The unique ID of the blob is returned.
	// NOTE: To use Content addressing and streaming we will need to chunk the data on the filesystem...
	Put(io.Reader) (id string, err error)
	// Delete a blob by its ID.
	Delete(id string) error
	// Get the content of a blob by its ID.
	Get(id string) (io.Reader, error)
	// LookupTag
	LookupTag(tag string) (string, error)
	// Tag a blob.
	Tag(id, tag string) error
	// DeleteTag deletes the tag reference to a blob, the underlying blob is not deleted.
	DeleteTag(tag string) error
	// Get the history of the blob IDs for a tag.
	TagHistory(tag string, count int) ([]TagEntry, error)
}

// TagEntry is a record of a tag to blob ID association
type TagEntry struct {
	Tag string
	// ID of the blob to which tag pointed.
	ID string
	// Time the tag was set.
	Time time.Time
	// Generation of the tag entry.
	Generation int64
}
