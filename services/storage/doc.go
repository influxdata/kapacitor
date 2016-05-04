/*
The storage package provides a key/value based interface for storing Kapacitor metadata.
All services wishing to store data should use this interface.

The usage patterns for this storage layer are typical create/replace/delete/get/list operations.
Typically objects are serialized and stored as the value.
As a result, updates to a single field of an object can incur the cost to retrieve the entire object and store it again.
In most cases this is acceptable since modifications are rare and object size is small.

A BoltDB backed implementation is also provided.

*/
package storage
