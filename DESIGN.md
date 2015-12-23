# Kapacitor Internal Design

This document is meant to layout both the high level design of Kapacitor as well and discuss the details
of the implementation.

It should be accessible to someone wanting to contribute to Kapacitor.


## Topic

* Key Concepts
* Data Flow -- How data flows through Kapacitor
* TICKscript -- How TICKscript is implemented (not written yet)
* kapacitord/kapacitor -- How the daemon ties all the pieces together. (not written yet)


## Key Concepts

Kapacitor is a framework for processing time series data.
It follows a [flow based programing](https://en.wikipedia.org/wiki/Flow-based_programming) model.
Data flows from node to node and each node is a *black box* process that can manipulate the data in any way.
The data model used to transport data from node to node matches the schema used by InfluxDB, namely measurements, tags and fields.
Nodes can be arranged in a directed acyclic graph [(DAG)](https://en.wikipedia.org/wiki/Directed_acyclic_graph).

### Tasks

Users define tasks for Kapacitor to run.
A task defines a DAG of nodes that process the data.
This task is defined via a DSL named TICKscript.
To learn more about how to use and interact with Kapacitor see the [docs](https://docs.influxdata.com/kapacitor/).

A task defines a potentially infinite amount of work to be done.
The amount work is determined by the data that is received by the task.
Once the source data stream is *closed* the task is complete.
It is normal for a task to never complete but rather run indefinitely.
As a result, tasks can be in one of three states, disabled, enabled not executing, and enabled executing.
An enabled task is not executing if it encountered an error or its data source was closed.

## Data Flow

Data flows from node to node and each node is a black box that can process the data the however it sees fit.
In order for a system like this to work the transport method and data model needs to be well defined.

### Models

The data model for transporting data has two types:

* Stream -- Data points are passed as single entities.
* Batch -- Data points are passed in groups of data.

A batch consists of a type that describes the common attributes of all data points within the batch
and a list of all the individual data points.

A data point consists of a timestamp, a map of fields, and a map of tags.
When data points are transfered as a stream not within the context of a batch they
also contain information on their scope, i.e database, retention policy and measurement.
This data model is schemaless in that the names of fields and tags are arbitrary and opaque to Kapacitor.

Lastly both batches and streamed data points contain information about the *group* they belong two if
the data set has been grouped by any number of dimensions. More on that later.

### Time

Time is measured based on the timestamps of the data flowing through a node.
If data flow stops so does time.
If a node performs a transformation dependent on time then it is always consistent based on a given data set.

### Edges

Kapacitor models data transfer along *edges*.
An edge connects exactly two nodes and data flows from the *parent* node to the *child* node.
There are two actions performed on an edge:

* Collect -- The parent presents a data point for the edge to consume.
* Emit -- The child retrieves a data point from the edge.

From the perspective of an *edge* data is collected from a parent and then emitted to a child.
From the perspective of a *node*, data is pulled off from any number of parent
edges and collected into any number of child edges.
Nodes, not edges, control the flow of the data. Edges simply provide the transport mechanism.
Meaning that if a child node stops pulling data from its parent edge, data flow stops.

Edges are typed, meaning that a given edge only transports a certain type of data, i.e. streams or batches.
Nodes are said to *want* parent edges of a certain type and to *provide* child edges of a certain type.
A node can want and provide the same or different type of edges. For example the `WindowNode` wants a stream edge
while providing a batch edge.

Modeling data flow through edges allows for the transport mechanism to be abstracted.
If the data is being transfered within the same process then it can be sent via in-memory structures;
if the data is being transfered to another Kapacitor host it can be serialized and transfered accordingly.

The current implementation of an edge uses Go channels and can be found in `edge.go`.
There are three channel per edge instance but only ever one channel is non nil based on the type of the edge.
Direct access to the channel is not provided but rather wrapper methods for collecting and emitting the data.
This allows for the edge to keep counts on throughput and be aborted at any point.
The channels are currently unbuffered, this will probably need to change eventually, but for now the simplicity is useful.

Passing batch data can be accomplished in one of two ways.
First, pass the data as a single object containing the complete batch and all points.
Second, pass marker objects that indicate the beginning and end of batches and stream individual points between the markers.
The marker objects can also contain the common data to the batch.
Currently the first option is used.
This has the advantage that fewer objects are passing through the channels.
It also works better with the current map-reduce functions in core sense they expect all the data in a single object.
It has the disadvantage that the whole batch has to be held in memory.
In some cases the entire batch does need to live in memory but not in all.
For example a node that is counting points per batch need only maintain a counter in memory and not the entire batch.

### Source Mapping

Kapacitor can receive data from many different sources, including querying InfluxDB.
The type TaskMaster in`task_master.go` is responsible for managing which tasks are receiving which data.

For stream tasks this is done by having on global edge.
All sources (graphite, collectd, http, etc) write their data to the TaskMaster, who writes the data to the global *stream* edge.
When a stream task is started it gets a *fork* of the global stream filtered down by the databases and retention policies its allowed to access.
Then the task can further process the data stream.

In the case of the batch tasks, the TaskMaster manages starting the schedules for querying InfluxDB.
The results of the queries are passed to the root nodes of the task directly.


### Windowing

Windowing data is an important piece to creating pipelines.
Windowing is concerned with how you can slice a data stream into multiple windows and is orthogonal to how batches are transfered.
Kapacitor handles windowing explicitly, by allowing the user to define a WindowNode
that has two parameters. First, the `period` is the length of the window in time.
Second, the `every` property defines how often an window should be emitted into the stream.
This allows for creating windows that overlap, have no overlap, or have gaps between the windows.
As a result the concept of a window does not exist inherently in the data stream, but rather windowing is the method of converting a stream of data into a batch of data.

Example TICKscript:

```javascript
stream
    .window()
        .period(10s)
        .every(5s)
```

The above script slices the incoming stream into overlapping windows.
Each window contains the last 10s of data and a new window is emitted every 5s.


### Challenges

Challenges with the current implementation:

* For stream tasks: If a single node stop processing data all nodes will eventually stop including nodes from other tasks.
    This is because of the global stream to aggregate all incoming sources and the fact the edges just block instead of dropping data.
    This could be mitigated further by creating independent streams for each database retention policy pair, but this only provides isolation and not a solution.
    We need a contract in place for what to do when a given node stops processing data.
* Nodes are responsible for not creating deadlock in the way they read and write data from their parent and child edges.
    For example the `JoinNode` has multiple parents and has to guarantee that the goroutines that are reading from the parents never block on each other.
    Otherwise a deadlock can be created since a parent may be blocked writing to the JoinNode while the JoinNode is blocked reading from a different parent.
    Since both parents could have a common ancestor the blocked parent will eventually block the ancestor which in turn will block the other parent.
* Fragile, so far the smallest of changes to the way the system work almost always results in a deadlock, because of the order of processing data.
* If data flow stops so does time. In many use cases this is exactly what you want, but in some cases you would still like the data in transit to be flushed out.
    As for monitoring the throughput of tasks this is possible out-of-band of the task so even if the task stop processing data you can still trigger an event in a different task.

