# File based task/template/handler definition

This proposal introduces directory based task, template, and handler definition.

## Configuration
The load service configuration is specified under the `load` tag of the
kapacitor configuration file.

```
[load]
 enabled = true
 dir="/path/to/directory"
```

`dir` specifies the directory where the definition files exist.

The service will attempt to load data from three subdirectories.

The `tasks` directory should contain task tickscripts and and the associated templated task
definition files (either yaml or json).

The `templates` directory should contain templated tickscripts.

The `handlers` directory will contain will contain topic handler definitions in yaml or json.

## Tasks

Task files must be placed in the `tasks` subdirectory of the load service
directory. Defining tasks explicitly will be done according to the following scheme:

* `id` - the file name without the tick extension
* `type` - determined by introspection of the task (stream, batch)
* `dbrp` - defined using the `dbrp` keyword followed by a specified database and retention policy

For example, the tickscript

```
// /path/to/directory/tasks/my_task.tick
dbrp "telegraf"."autogen"

stream
    |from()
        .measurement('cpu')
        .groupBy(*)
    |alert()
        .warn(lambda: "usage_idle" < 20)
        .crit(lambda: "usage_idle" < 10)
        // Send alerts to the `cpu` topic
        .topic('cpu')
```

will create a `stream` task named `my_task` for the dbrp `telegraf.autogen`.


## Templates

Template files must be placed in the `templates` subdirectory of the load service
directory. Defining templated tasks is done according to the following scheme:

* `id` - the file name without the tick extension
* `type` - determined by introspection of the task (stream, batch)
* `dbrp` - defined using the `dbrp` keyword followed by a specified database and retention policy

For example, the tickscript
```
// /path/to/directory/templates/my_template.tick
dbrp "telegraf"."autogen"

var measurement string
var where_filter = lambda: TRUE
var groups = [*]
var field string
var warn lambda
var crit lambda
var window = 5m
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

will create a `stream` template named `my_template` for the dbrp `telegaf.autogen`.

### Templated Tasks

Templated task files must be placed in the `tasks` subdirectory of the load service
directory. Defining templated tasks will be done according to the following scheme:

* `id` - filename without the `yaml`, `yml`, or `json` extension
* `dbrps` - required if not specified in template
* `template-id` - required
* `vars` - list of template vars

For example, the templated task file

```yaml
# /path/to/directory/tasks/my_templated_tas.tick
dbrps:
  - { db: "telegraf", rp: "autogen"}
template-id: my_template
vars:
  measurement:
   type: string
   value: cpu
  where_filter:
   type: lambda
   value: "\"cpu\" == 'cpu-total'"
  groups:
   type: list
   value:
       - type: string
         value: host
       - type: string
         value: dc
  field:
   type: string
   value : usage_idle
  warn:
   type: lambda
   value: "\"mean\" < 30.0"
  crit:
   type: lambda
   value: "\"mean\" < 10.0"
  window:
   type: duration
   value : 1m
  slack_channel:
   type: string
   value: "#alerts_testing"
```
will create a `stream` task named `my_templated_task` for the dbrp `telegraf.autogen`.

The same task may be created using JSON like so:

```json
{
  "dbrps": [{"db": "telegraf", "rp": "autogen"}],
  "template-id": "my_template",
  "vars": {
    "measurement": {"type" : "string", "value" : "cpu" },
    "where_filter": {"type": "lambda", "value": "\"cpu\" == 'cpu-total'"},
    "groups": {"type": "list", "value": [{"type":"string", "value":"host"},{"type":"string", "value":"dc"}]},
    "field": {"type" : "string", "value" : "usage_idle" },
    "warn": {"type" : "lambda", "value" : "\"mean\" < 30.0" },
    "crit": {"type" : "lambda", "value" : "\"mean\" < 10.0" },
    "window": {"type" : "duration", "value" : "1m" },
    "slack_channel": {"type" : "string", "value" : "#alerts_testing" }
  }
}
```

## Handlers

Topic Handler files must be placed in the `handlers` subdirectory of the load service
directory.

```
id: hanlder-id
topic: cpu
kind: slack
match: changed() == TRUE
options:
  channel: '#alerts'
```

