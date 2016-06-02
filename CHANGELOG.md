# Changelog

## v0.14.0 [unreleased]

### Release Notes

#### Template Tasks

The ability to create and use template tasks has been added.
you can define a template for a task and reuse that template across multiple tasks.

A simple example:

```go
// Which measurement to consume
var measurement string
// Optional where filter
var where_filter = lambda: TRUE
// Optional list of group by dimensions
var groups = [*]
// Which field to process
var field string
// Warning criteria, has access to 'mean' field
var warn lambda
// Critical criteria, has access to 'mean' field
var crit lambda
// How much data to window
var window = 5m
// The slack channel for alerts
var slack_channel = '#alerts'

stream
    |from()
        .measurement(measurement)
        .where(where_filter)
        .groupBy(groups)
    |window()
        .period(window)
        .every(window)
    |mean(field)
    |alert()
         .warn(warn)
         .crit(crit)
         .slack()
         .channel(slack_channel)
```

Then you can define the template like so:

```
kapacitor define-template generic_mean_alert -tick path/to/above/script.tick -type stream
```

Next define a task that uses the template:

```
kapacitor define cpu_alert -template-id generic_mean_alert -vars cpu_vars.json -dbrp telegraf.default
```

Where `cpu_vars.json` would like like this:

```json
{
    "measurement": {"type" : "string", "value" : "cpu" },
    "where_filter": {"type": "lambda", "value": "\"cpu\" == 'cpu-total'"},
    "groups": {"type": "list", "value": [{"type":"string", "value":"host"},{"type":"string", "value":"dc"}]},
    "field": {"type" : "string", "value" : "usage_idle" },
    "warn": {"type" : "lambda", "value" : " \"mean\" < 30.0" },
    "crit": {"type" : "lambda", "value" : " \"mean\" < 10.0" },
    "window": {"type" : "duration", "value" : "1m" },
    "slack_channel": {"type" : "string", "value" : "#alerts_testing" }
}
```


#### Live Replays

With this release you can now replay data directly against a task from InfluxDB without having to first create a recording.
Replay the queries defined in the batch task `cpu_alert` for the past 10 hours.
```sh
kapacitor replay-live batch -task cpu_alert -past 10h
```

Or for a stream task with use a query directly:

```sh
kapacitor replay-live query -task cpu_alert -query 'SELECT usage_idle FROM telegraf."default".cpu WHERE time > now() - 10h'
```

#### HTTP based subscriptions

Now InfluxDB and Kapacitor support HTTP/S based subscriptions.
This means that Kapacitor need only listen on a single port for the HTTP service, greatly simplifying configuration and setup.

In order to start using HTTP subscriptions change the `subscription-protocol` option for your configured InfluxDB clusters.

For example:

```
[[influxdb]]
  enabled = true
  urls = ["http://localhost:8086",]
  subscription-protocol = "http"
  # or to use https
  #subscription-protocol = "https"
```

On startup Kapacitor will detect the change and recreate the subscriptions in InfluxDB to use the HTTP protocol.

>NOTE: While HTTP itself is a TCP transport such that packet loss shouldn't be an issue, if Kapacitor starts to slow down for whatever reason, InfluxDB will drop the subscription writes to Kapacitor.
In order to know if subscription writes are being dropped you should monitor the measurement `_internal.monitor.subscriber` for the field `writeFailures`.


### Features

- [#283](https://github.com/influxdata/kapacitor/issues/283): Add live replays.
- [#500](https://github.com/influxdata/kapacitor/issues/500): Support Float,Integer,String and Boolean types.
- [#82](https://github.com/influxdata/kapacitor/issues/82): Multiple services for PagerDuty alert.
- [#558](https://github.com/influxdata/kapacitor/pull/558): Preserve fields as well as tags on selector InfluxQL functions.
- [#259](https://github.com/influxdata/kapacitor/issues/259): Template Tasks have been added.
- [#562](https://github.com/influxdata/kapacitor/pull/562): HTTP based subscriptions.
- [#595](https://github.com/influxdata/kapacitor/pull/595): Support counting and summing empty batches to 0.
- [#596](https://github.com/influxdata/kapacitor/pull/596): Support new group by time offset i.e. time(30s, 5s)
- [#416](https://github.com/influxdata/kapacitor/issues/416): Track ingress counts by database, retention policy, and measurement. Expose stats via cli.


### Bugfixes

- [#540](https://github.com/influxdata/kapacitor/issues/540): Fixes bug with log level API endpoint.
- [#521](https://github.com/influxdata/kapacitor/issues/521): EvalNode now honors groups.
- [#561](https://github.com/influxdata/kapacitor/issues/561): Fixes bug when lambda expressions would return error about types with nested binary expressions.
- [#555](https://github.com/influxdata/kapacitor/issues/555): Fixes bug where "time" functions didn't work in lambda expressions.
- [#570](https://github.com/influxdata/kapacitor/issues/570): Removes panic in SMTP service on failed close connection.
- [#587](https://github.com/influxdata/kapacitor/issues/587): Allow number literals without leading zeros.
- [#584](https://github.com/influxdata/kapacitor/issues/584): Do not block during startup to send usage stats.

## v0.13.1 [2016-05-13]

### Release Notes

>**Breaking changes may require special upgrade steps from versions <= 0.12, please read the 0.13.0 release notes**

Along with the API changes of 0.13.0, validation logic was added to task IDs, but this was not well documented.
This minor release remedies that.

All IDs (tasks, recordings, replays) must match this regex `^[-\._\p{L}0-9]+$`, which is essentially numbers, unicode letters, '-', '.' and '_'.

If you have existing tasks which do not match this pattern they should continue to function normally.

### Features


### Bugfixes

- [#545](https://github.com/influxdata/kapacitor/issues/545): Fixes inconsistancy with API docs for creating a task.
- [#544](https://github.com/influxdata/kapacitor/issues/544): Fixes issues with existings tasks and invalid names.
- [#543](https://github.com/influxdata/kapacitor/issues/543): Fixes default values not being set correctly in API calls.


## v0.13.0 [2016-05-11]

### Release Notes

>**Breaking changes may require special upgrade steps please read below.**

#### Upgrade Steps

Changes to how and where task data is store have been made.
In order to safely upgrade to version 0.13 you need to follow these steps:

1. Upgrade InfluxDB to version 0.13 first.
2. Update all TICKscripts to use the new `|` and `@` operators. Once Kapacitor no longer issues any `DEPRECATION` warnings you are ready to begin the upgrade.
The upgrade will work without this step but tasks using the old syntax cannot be enabled, until modified to use the new syntax.
3. Upgrade the Kapacitor binary/package.
4. Configure new database location. By default the location `/var/lib/kapacitor/kapacitor.db` is chosen for package installs or `./kapacitor.db` for manual installs.
Do **not** remove the configuration for the location of the old task.db database file since it is still needed to do the migration.

    ```
    [storage]
    boltdb = "/var/lib/kapacitor/kapacitor.db"
    ```

5. Restart Kapacitor. At this point Kapacitor will migrate all existing data to the new database file.
If any errors occur Kapacitor will log them and fail to startup. This way if Kapacitor starts up you can be sure the migration was a success and can continue normal operation.
The old database is opened in read only mode so that existing data cannot be corrupted.
Its recommended to start Kapacitor in debug logging mode for the migration so you can follow the details of the migration process.

At this point you may remove the configuration for the old `task` `dir` and restart Kapacitor to ensure everything is working.
Kapacitor will attempt the migration on every startup while the old configuration and db file exist, but will skip any data that was already migrated.


#### API Changes

With this release the API has been updated to what we believe will be the stable version for a 1.0 release.
Small changes may still be made but the significant work to create a RESTful HTTP API is complete.
Many breaking changes introduced, see the [client/API.md](http://github.com/influxdata/kapacitor/blob/master/client/API.md) doc for details on how the API works now.

#### CLI Changes

Along with the API changes, breaking changes where also made to the `kapacitor` CLI command.
Here is a break down of the CLI changes:

* Every thing has an ID now: tasks, recordings, even replays.
    The `name` used before to define a task is now its `ID`.
    As such instead of using `-name` and `-id` to refer to tasks and recordings,
    the flags have been changed to `-task` and `-recording` accordingly.
* Replays can be listed and deleted like tasks and recordings.
* Replays default to `fast` clock mode.
* The record and replay commands now have a `-no-wait` option to start but not wait for the recording/replay to complete.
* Listing recordings and replays displays the status of the respective action.
* Record and Replay command now have an optional flag `-replay-id`/`-recording-id` to specify the ID of the replay or recording.
    If not set then a random ID will be chosen like the previous behavior.

#### Notable features

UDF can now be managed externally to Kapacitor via Unix sockets.
A process or container can be launched independent of Kapacitor exposing a socket.
On startup Kapacitor will connect to the socket and begin communication.

Example UDF config for a socket based UDF.

```
[udf]
[udf.functions]
    [udf.functions.myCustomUDF]
       socket = "/path/to/socket"
       timeout = "10s"
```

Alert data can now be consumed directly from within TICKscripts.
For example, let's say we want to store all data that triggered an alert in InfluxDB with a tag `level` containing the level string value (i.e CRITICAL).

```javascript
...
    |alert()
        .warn(...)
        .crit(...)
        .levelTag('level')
        // and/or use a field
        //.levelField('level')
        // Also tag the data with the alert ID
        .idTag('id')
        // and/or use a field
        //.idField('id')
    |influxDBOut()
        .database('alerts')
        ...
```


### Features

- [#360](https://github.com/influxdata/kapacitor/pull/360): Forking tasks by measurement in order to improve performance
- [#386](https://github.com/influxdata/kapacitor/issues/386): Adds official Go HTTP client package.
- [#399](https://github.com/influxdata/kapacitor/issues/399): Allow disabling of subscriptions.
- [#417](https://github.com/influxdata/kapacitor/issues/417): UDFs can be connected over a Unix socket. This enables UDFs from across Docker containers.
- [#451](https://github.com/influxdata/kapacitor/issues/451): StreamNode supports `|groupBy` and `|where` methods.
- [#93](https://github.com/influxdata/kapacitor/issues/93): AlertNode now outputs data to child nodes. The output data can have either a tag or field indicating the alert level.
- [#281](https://github.com/influxdata/kapacitor/issues/281): AlertNode now has an `.all()` property that specifies that all points in a batch must match the criteria in order to trigger an alert.
- [#384](https://github.com/influxdata/kapacitor/issues/384): Add `elapsed` function to compute the time difference between subsequent points.
- [#230](https://github.com/influxdata/kapacitor/issues/230): Alert.StateChangesOnly now accepts optional duration arg. An alert will be triggered for every interval even if the state has not changed.
- [#426](https://github.com/influxdata/kapacitor/issues/426): Add `skip-format` query parameter to the `GET /task` endpoint so that returned TICKscript content is left unmodified from the user input.
- [#388](https://github.com/influxdata/kapacitor/issues/388): The duration of an alert is now tracked and exposed as part of the alert data as well as can be set as a field via `.durationField('duration')`.
- [#486](https://github.com/influxdata/kapacitor/pull/486): Default config file location.
- [#461](https://github.com/influxdata/kapacitor/pull/461): Make Alerta `event` property configurable.
- [#491](https://github.com/influxdata/kapacitor/pull/491): BREAKING: Rewriting stateful expression in order to improve performance, the only breaking change is: short circuit evaluation for booleans - for example: ``lambda: "bool_value" && (count() > 100)`` if "bool_value" is false, we won't evaluate "count".
- [#504](https://github.com/influxdata/kapacitor/pull/504): BREAKING: Many changes to the API and underlying storage system. This release requires a special upgrade process.
- [#511](https://github.com/influxdata/kapacitor/pull/511): Adds DefaultNode for providing default values for missing fields or tags.
- [#285](https://github.com/influxdata/kapacitor/pull/285): Track created,modified and last enabled dates on tasks.
- [#533](https://github.com/influxdata/kapacitor/pull/533): Add useful statistics for nodes.

### Bugfixes

- [#499](https://github.com/influxdata/kapacitor/issues/499): Fix panic in InfluxQL nodes if field is missing or incorrect type.
- [#441](https://github.com/influxdata/kapacitor/issues/441): Fix panic in UDF code.
- [#429](https://github.com/influxdata/kapacitor/issues/429): BREAKING: Change TICKscript parser to be left-associative on equal precedence operators. For example previously this statement `(1+2-3*4/5)` was evaluated as `(1+(2-(3*(4/5))))`
    which is not the typical/expected behavior. Now using left-associative parsing the statement is evaluated as `((1+2)-((3*4)/5))`.
- [#456](https://github.com/influxdata/kapacitor/pull/456): Fixes Alerta integration to let server set status, fix `rawData` attribute and set default severity to `indeterminate`.
- [#425](https://github.com/influxdata/kapacitor/pull/425): BREAKING: Preserving tags on influxql simple selectors - first, last, max, min, percentile
- [#423](https://github.com/influxdata/kapacitor/issues/423): Recording stream queries with group by now correctly saves data in time order not group by order.
- [#331](https://github.com/influxdata/kapacitor/issues/331): Fix panic when missing `.as()` for JoinNode.
- [#523](https://github.com/influxdata/kapacitor/pull/523): JoinNode will now emit join sets as soon as they are ready. If multiple joinable sets arrive in the same tolerance window than each will be emitted (previously the first points were dropped).
- [#537](https://github.com/influxdata/kapacitor/issue/537): Fix panic in alert node when batch is empty.

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


