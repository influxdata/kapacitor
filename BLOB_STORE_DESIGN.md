# Blob Store

The blob store is a mechanism to store arbitrary data in Kapacitor.
The data stored is immutable and opaque to Kapacitor.

Data is stored as blobs.
A naming system is used to refer various blobs within the store.
Names can be modified and the history of a name/blob associations are recorded.

There are no specific limits on the size of a blob, and blobs can be streamed via chunks in and out of the store.

## Uses

The following details the various uses of the Kapacitor blob store.

### Snapshots

Kapacitor will periodically snapshot the state of a running task. (Currently only implemented for UDFs).
When a task is started its previous snapshot or a named snapshot is restored.

Kapacitor tasks construct a [DAG](https://en.wikipedia.org/wiki/Directed_acyclic_graph) of the data pipeline.
Each step in this DAG is called a node.
Snapshots are associated with a single node within a single task.
All nodes are assigned IDs based on the DAG structure.
When the DAG changes the previous snapshots are considered invalid an are no longer used to restore task state.

### UDFs

UDFs can explicitly save and request blobs from the store via the protobuf socket connection with Kapacitor.
A common use case is to load and store trained model data.
However you use the blob store within your UDF is up to you.


## Design

The blob store will use content addressable IDs(i.e. shasum of the content) and be exposed via the HTTP API of Kapacitor.

Blobs can be created, named and deleted.
Creating a blob will accept only the content of the blob data and return the ID of the blob.
Naming a blob associates a specified name to the content of the blob.
A naming history is recorded, allowing the users to determine the "version" history for a given name.
Deleting a blob removes it from the store.

Blobs will use a chunked/framed transport so that the size of blobs need not be overly constrained by available memory.

