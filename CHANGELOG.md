# Changelog

## v0.13.0 [unreleased]

### Release Notes

### Features

### Bugfixes

## v0.12.0 [2016-04-04]

### Release Notes

New TICKscript syntax that uses a different operators for chaining methods vs property methods vs UDF methods.

* A chaining method is a method that creates a new node in the pipeline. Uses the `|` operator.
* A property method is a method that changes a property on a node. Uses the `.` operator.
* A UDF method is a method that calls out to a UDF. Uses the `@` operator.

For example below the `from`, `mean`, and `alert` methods create new nodes,
the `detectAnomalies` method calls a UDF,
and the other methods modify the nodes as property methods.

```javascript
stream
    |from()
        .measurement('cpu')
        .where(lambda: "cpu" == 'cpu-total')
    |mean('usage_idle')
        .as('value')
    @detectAnomalies()
        .field('mean')
    |alert()
        .crit(lambda: "anomaly_score" > 10)
        .log('/tmp/cpu.log')
```

With this change a new binary is provided with Kapacitor `tickfmt` which will
format a TICKscript file according to a common standard.


### Features

- [#299](https://github.com/influxdata/kapacitor/issues/299): Changes TICKscript chaining method operators and adds `tickfmt` binary.
- [#389](https://github.com/influxdata/kapacitor/pull/389): Adds benchmarks to Kapacitor for basic use cases.
- [#390](https://github.com/influxdata/kapacitor/issues/390): BREAKING: Remove old `.mapReduce` functions.
- [#381](https://github.com/influxdata/kapacitor/pull/381): Adding enable/disable/delete/reload tasks by glob.
- [#401](https://github.com/influxdata/kapacitor/issues/401): Add `.align()` property to BatchNode so you can align query start and stop times.

### Bugfixes

- [#378](https://github.com/influxdata/kapacitor/issues/378): Fix issue where derivative would divide by zero.
- [#387](https://github.com/influxdata/kapacitor/issues/387): Add `.quiet()` option to EvalNode so errors can be suppressed if expected.
- [#400](https://github.com/influxdata/kapacitor/issues/400): All query/connection errors are counted and reported in BatchNode stats.
- [#412](https://github.com/influxdata/kapacitor/pull/412): Fix issues with batch queries dropping points because of nil fields.
- [#413](https://github.com/influxdata/kapacitor/pull/413): Allow disambiguation between ".groupBy" and "|groupBy".


## v0.11.0 [2016-03-22]

### Release Notes

Kapacitor is now using the functions from the new query engine in InfluxDB core.
Along with this change is a change in the TICKscript API so that using the InfluxQL functions is easier.
Simply call the desired method directly no need to call `.mapReduce` explicitly.
This change now hides the mapReduce aspect and handles it internally.
Using `.mapReduce` is officially deprecated in this release and will be remove in the next major release.
We feel that this change improves the readability of TICKscripts and exposes less implementation details
to the end user.
Updating your exising TICKscripts is simple.
If previously you had code like this:

```javascript
stream.from()...
    .window()...
    .mapReduce(influxql.count('value'))
```
then update it to look like this:

```javascript
stream.from()...
    .window()...
    .count('value')
```

a simple regex could fix all your existing scripts.

Kapacitor now exposes more internal metrics for determining the performance of a given task.
The internal statistics includes a new measurement named `node` that contains any stats a node provides, tagged by the task, node, task type and kind of node (i.e. window vs union).
All nodes provide an averaged execution time for the node.
These stats are also available in the DOT output of the Kapacitor show command.

Significant performance improvements have also been added.
In some cases Kapacitor throughput has improved by 4X.

Kapacitor can now connect to different InfluxDB clusters.
Multiple InfluxDB config sections can be defined and one will be marked as default.
To upgrade convert an `influxdb` config.

From this:

```
[influxdb]
  enabled = true
  ...
```

to this:

```
[[influxdb]]
  enabled = true
  default = true
  name = "localhost"
  ...
```

Various improvements to joining features have been implemented.
With #144 you can now join streams with differing group by dimensions.

If you previously configured Email, Slack or HipChat globally now you must also set the `state-changes-only` option to true as well if you want to preserve the original behavior.
For example:

```
[slack]
   enable = true
   global = true
   state-changes-only = true
```

### Features
- [#236](https://github.com/influxdata/kapacitor/issues/236): Implement batched group by
- [#231](https://github.com/influxdata/kapacitor/pull/231): Add ShiftNode so values can be shifted in time for joining/comparisons.
- [#190](https://github.com/influxdata/kapacitor/issues/190): BREAKING: Deadman's switch now triggers off emitted counts and is grouped by to original grouping of the data.
    The breaking change is that the 'collected' stat is no longer output for `.stats` and has been replaced by `emitted`.
- [#145](https://github.com/influxdata/kapacitor/issues/145): The InfluxDB Out Node now writes data to InfluxDB in buffers.
- [#215](https://github.com/influxdata/kapacitor/issues/215): Add performance metrics to nodes for average execution times and node throughput values.
- [#144](https://github.com/influxdata/kapacitor/issues/144): Can now join streams with differing dimensions using the join.On property.
- [#249](https://github.com/influxdata/kapacitor/issues/249): Can now use InfluxQL functions directly instead of via the MapReduce method. Example `stream.from().count()`.
- [#233](https://github.com/influxdata/kapacitor/issues/233): BREAKING: Now you can use multiple InfluxDB clusters. The config changes to make this possible are breaking. See notes above for changes.
- [#302](https://github.com/influxdata/kapacitor/issues/302): Can now use .Time in alert message.
- [#239](https://github.com/influxdata/kapacitor/issues/239): Support more detailed TLS config when connecting to an InfluxDB host.
- [#323](https://github.com/influxdata/kapacitor/pull/323): Stats for task execution are provided via JSON HTTP request instead of just DOT string. thanks @yosiat
- [#358](https://github.com/influxdata/kapacitor/issues/358): Improved logging. Adds LogNode so any data in a pipeline can be logged.
- [#366](https://github.com/influxdata/kapacitor/issues/366): HttpOutNode now allows chaining methods.


### Bugfixes
- [#199](https://github.com/influxdata/kapacitor/issues/199): BREAKING: Various fixes for the Alerta integration.
    The `event` property has been removed from the Alerta node and is now set as the value of the alert ID.
- [#232](https://github.com/influxdata/kapacitor/issues/232): Better error message for alert integrations. Better error message for VictorOps 404 response.
- [#231](https://github.com/influxdata/kapacitor/issues/231): Fix window logic when there were gaps in the data stream longer than window every value.
- [#213](https://github.com/influxdata/kapacitor/issues/231): Add SourceStreamNode so that yuou must always first call `.from` on the `stream` object before filtering it, so as to not create confusing to understand TICKscripts.
- [#255](https://github.com/influxdata/kapacitor/issues/255): Add OPTIONS handler for task delete method so it can be preflighted.
- [#258](https://github.com/influxdata/kapacitor/issues/258): Fix UDP internal metrics, change subscriptions to use clusterID.
- [#240](https://github.com/influxdata/kapacitor/issues/240): BREAKING: Fix issues with Sensu integration. The breaking change is that the config no longer takes a `url` but rather a `host` option since the communication is raw TCP rather HTTP.
- [#270](https://github.com/influxdata/kapacitor/issues/270): The HTTP server will now gracefully stop.
- [#300](https://github.com/influxdata/kapacitor/issues/300): Add OPTIONS method to /recording endpoint for deletes.
- [#304](https://github.com/influxdata/kapacitor/issues/304): Fix panic if recording query but do not have an InfluxDB instance configured
- [#289](https://github.com/influxdata/kapacitor/issues/289): Add better error handling to batch node.
- [#142](https://github.com/influxdata/kapacitor/issues/142): Fixes bug when defining multiple influxdb hosts.
- [#266](https://github.com/influxdata/kapacitor/issues/266): Fixes error log for HipChat that is not an error.
- [#333](https://github.com/influxdata/kapacitor/issues/333): Fixes hang when replaying with .stats node. Fixes issues with batch and stats.
- [#340](https://github.com/influxdata/kapacitor/issues/340): BREAKING: Decouples global setting for alert handlers from the state changes only setting.
- [#348](https://github.com/influxdata/kapacitor/issues/348): config.go: refactor to simplify structure and fix support for array elements
- [#362](https://github.com/influxdata/kapacitor/issues/362): Fix bug with join tolerance and batches.

## v0.10.1 [2016-02-08]

### Release Notes

This is a bug fix release that fixes many issues releated to the recent 0.10.0 release.
The few additional features are focused on usability improvements from recent feedback.

Improved UDFs, lots of bug fixes and improvements on the API. There was a breaking change for UDFs protobuf messages, see #176.

There was a breaking change to the `define` command, see [#173](https://github.com/influxdata/kapacitor/issues/173) below.

### Features

- [#176](https://github.com/influxdata/kapacitor/issues/176): BREAKING: Improved UDFs and groups. Now it is easy to deal with groups from the UDF process.
    There is a breaking change in the BeginBatch protobuf message for this change.
- [#196](https://github.com/influxdata/kapacitor/issues/196): Adds a 'details' property to the alert node so that the email body can be defined. See also [#75](https://github.com/influxdata/kapacitor/issues/75).
- [#132](https://github.com/influxdata/kapacitor/issues/132): Make is so multiple calls to `where` simply `AND` expressions together instead of replacing or creating extra nodes in the pipeline.
- [#173](https://github.com/influxdata/kapacitor/issues/173): BREAKING: Added a `-no-reload` flag to the define command in the CLI. Now if the task is enabled define will automatically reload it unless `-no-reload` is passed.
- [#194](https://github.com/influxdata/kapacitor/pull/194): Adds Talk integration for alerts. Thanks @wutaizeng!
- [#320](https://github.com/influxdata/kapacitor/pull/320): Upgrade to go 1.6

### Bugfixes

- [#177](https://github.com/influxdata/kapacitor/issues/177): Fix panic for show command on batch tasks.
- [#185](https://github.com/influxdata/kapacitor/issues/185): Fix panic in define command with invalid dbrp value.
- [#195](https://github.com/influxdata/kapacitor/issues/195): Fix panic in where node.
- [#208](https://github.com/influxdata/kapacitor/issues/208): Add default stats dbrp to default subscription excludes.
- [#203](https://github.com/influxdata/kapacitor/issues/203): Fix hang when deleteing invalid batch task.
- [#182](https://github.com/influxdata/kapacitor/issues/182): Fix missing/incorrect Content-Type headers for various HTTP endpoints.
- [#187](https://github.com/influxdata/kapacitor/issues/187): Retry connecting to InfluxDB on startup for up to 5 minutes by default.

## v0.10.0 [2016-01-26]

### Release Notes

This release marks the next major release of Kapacitor.
With this release you can now run your own custom code for processing data within Kapacitor.
See [udf/agent/README.md](https://github.com/influxdata/kapacitor/blob/master/udf/agent/README.md) for more details.

With the addition of UDFs it is now possible to run custom anomaly detection alogrithms suited to your needs.
There are simple examples of how to use UDFs in [udf/agent/examples](https://github.com/influxdata/kapacitor/tree/master/udf/agent/examples/).

The version has jumped significantly so that it is inline with other projects in the TICK stack.
This way you can easily tell which versions of Telegraf, InfluxDB, Chronograf and Kapacitor work together.

See note on a breaking change in the HTTP API below. #163


### Features
- [#137](https://github.com/influxdata/kapacitor/issues/137): Add deadman's switch. Can be setup via TICKscript and globally via configuration.
- [#72](https://github.com/influxdata/kapacitor/issues/72): Add support for User Defined Functions (UDFs).
- [#139](https://github.com/influxdata/kapacitor/issues/139): Alerta.io support thanks! @md14454
- [#85](https://github.com/influxdata/kapacitor/issues/85): Sensu support using JIT clients. Thanks @sstarcher!
- [#141](https://github.com/influxdata/kapacitor/issues/141): Time of day expressions for silencing alerts.

### Bugfixes
- [#153](https://github.com/influxdata/kapacitor/issues/153): Fix panic if referencing non existant field in MapReduce function.
- [#138](https://github.com/influxdata/kapacitor/issues/138): Change over to influxdata github org.
- [#164](https://github.com/influxdata/kapacitor/issues/164): Update imports etc from InfluxDB as per the new meta store/client changes.
- [#163](https://github.com/influxdata/kapacitor/issues/163): BREAKING CHANGE: Removed the 'api/v1' pathing from the HTTP API so that Kapacitor is 
    path compatible with InfluxDB. While this is a breaking change the kapacitor cli has been updated accordingly and you will not experience any distruptions unless you 
    were calling the HTTP API directly.
- [#147](https://github.com/influxdata/kapacitor/issues/147): Compress .tar archives from builds.

## v0.2.4 [2016-01-07]

### Release Notes

### Features
- [#118](https://github.com/influxdata/kapacitor/issues/118): Can now define multiple handlers of the same type on an AlertNode.
- [#119](https://github.com/influxdata/kapacitor/issues/119): HipChat support thanks! @ericiles *2
- [#113](https://github.com/influxdata/kapacitor/issues/113): OpsGenie support thanks! @ericiles
- [#107](https://github.com/influxdata/kapacitor/issues/107): Enable TICKscript variables to be defined and then referenced from lambda expressions.
        Also fixes various bugs around using regexes.

### Bugfixes
- [#124](https://github.com/influxdata/kapacitor/issues/124): Fix panic where there is an error starting a task.
- [#122](https://github.com/influxdata/kapacitor/issues/122): Fixes panic when using WhereNode.
- [#128](https://github.com/influxdata/kapacitor/issues/128): Fix not sending emails when using recipient list from config.

## v0.2.3 [2015-12-22]

### Release Notes

Bugfix #106 made a breaking change to the internal HTTP API. This was to facilitate integration testing and overall better design.
Now POSTing a recording request will start the recording and immediately return. If you want to wait till it is complete do
a GET for the recording info and it will block until its complete. The kapacitor cli has been updated accordingly.

### Features
- [#96](https://github.com/influxdata/kapacitor/issues/96): Use KAPACITOR_URL env var for setting the kapacitord url in the client.
- [#109](https://github.com/influxdata/kapacitor/pull/109): Add throughput counts to DOT format in `kapacitor show` command, if task is executing.

### Bugfixes
- [#102](https://github.com/influxdata/kapacitor/issues/102): Fix race when start/stoping timeTicker in batch.go
- [#106](https://github.com/influxdata/kapacitor/pull/106): Fix hang when replaying stream recording.


## v0.2.2 [2015-12-16]

### Release Notes

Some bug fixes including one that cause Kapacitor to deadlock.

### Features
- [#83](https://github.com/influxdata/kapacitor/pull/83): Use enterprise usage client, remove deprecated enterprise register and reporting features.

### Bugfixes

- [#86](https://github.com/influxdata/kapacitor/issues/86): Fix dealock form errors in tasks. Also fixes issue where task failures did not get logged.
- [#95](https://github.com/influxdata/kapacitor/pull/95): Fix race in bolt usage when starting enabled tasks at startup.

## v0.2.0 [2015-12-8]

### Release Notes

Major public release.


