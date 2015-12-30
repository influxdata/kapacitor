# Changelog

## v0.2.4 [unreleased]

### Release Notes

### Features
- [#118](https://github.com/influxdb/kapacitor/issues/118): Can now define multiple handlers of the same type on an AlertNode.
- [#119](https://github.com/influxdb/kapacitor/issues/119): HipChat support thanks! @ericiles *2
- [#113](https://github.com/influxdb/kapacitor/issues/113): OpsGenie support thanks! @ericiles
- [#107](https://github.com/influxdb/kapacitor/issues/107): Enable TICKscript variables to be defined and then referenced from lambda expressions.
        Also fixes various bugs around using regexes.

### Bugfixes

## v0.2.3 [2015-12-22]

### Release Notes

Bugfix #106 made a breaking change to the internal HTTP API. This was to facilitate integration testing and overall better design.
Now POSTing a recording request will start the recording and immediately return. If you want to wait till it is complete do
a GET for the recording info and it will block until its complete. The kapacitor cli has been updated accordingly.

### Features
- [#96](https://github.com/influxdb/kapacitor/issues/96): Use KAPACITOR_URL env var for setting the kapacitord url in the client.
- [#109](https://github.com/influxdb/kapacitor/pull/109): Add throughput counts to DOT format in `kapacitor show` command, if task is executing.

### Bugfixes
- [#102](https://github.com/influxdb/kapacitor/issues/102): Fix race when start/stoping timeTicker in batch.go
- [#106](https://github.com/influxdb/kapacitor/pull/106): Fix hang when replaying stream recording.


## v0.2.2 [2015-12-16]

### Release Notes

Some bug fixes including one that cause Kapacitor to deadlock.

### Features
- [#83](https://github.com/influxdb/kapacitor/pull/83): Use enterprise usage client, remove deprecated enterprise register and reporting features.

### Bugfixes

- [#86](https://github.com/influxdb/kapacitor/issues/86): Fix dealock form errors in tasks. Also fixes issue where task failures did not get logged.
- [#95](https://github.com/influxdb/kapacitor/pull/95): Fix race in bolt usage when starting enabled tasks at startup.

## v0.2.0 [2015-12-8]

### Release Notes

Major public release.


