# Kapacitor API Reference Documentation

* [General Information](#general-information)
* [Writing Data](#writing-data)
* [Tasks](#tasks)
* [Templates](#templates)
* [Recordings](#recordings)
* [Replays](#replays)
* [Alerts](#alerts)
* [Configuration](#configuration)
* [Testing Services](#testing-services)
* [Miscellaneous](#miscellaneous)

## General Information

Kapacitor provides an HTTP API on port 9092 by default.
With the API you can control which tasks are executing, query status of tasks and manage recordings etc.

Each section below defines the available API endpoints and there inputs and outputs.

All requests are versioned and namespaced using the base path `/kapacitor/v1/`.

### Response Codes

All requests can return these response codes:

| HTTP Response Code | Meaning                                                                                                                                                             |
| ------------------ | -------                                                                                                                                                             |
| 2xx                | The request was a success, content is dependent on the request.                                                                                                     |
| 4xx                | Invalid request, refer to error for what it wrong with the request. Repeating the request will continue to return the same error.                                   |
| 5xx                | The server was unable to process the request, refer to the error for a reason. Repeating the request may result in a success if the server issue has been resolved. |

### Errors

All requests can return JSON in the following format to provide more information about a failed request.

```
{
    "error" : "error message"
}
```

### Query Parameters vs JSON body

To make using this API a consistent and easy experience we follow one simple rule for when extra information
about a request is found in the query parameters of the URL or when they are part of the submitted JSON body.

Query parameters are used only for GET requests and all other requests expect parameters to be specified in the JSON body.

>NOTE: The /kapacitor/v1/write endpoint is the one exception to this rule since Kapacitor is compatible with the InfluxDB /write endpoint.


### Links

When creating resources in Kapacitor the API server will return a `link` object with an `href` of the resource.
Clients should not need to perform path manipulation in most cases and can use the links provided from previous calls.

### IDs

The API allows the client to specify IDs for the various resources.
This way you can control the meaning of the IDs.
If you do not specify an ID a random UUID will be generated for the resource.

All IDs must match this regex `^[-\._\p{L}0-9]+$`, which is essentially numbers, unicode letters, '-', '.' and '_'.


### Backwards Compatibility

Currently Kapacitor is in 1.x release with a guarantee that all new releases will be backwards compatible with previous releases.
This applies directly to the API. New additions may be made to the API but existing endpoints will not be changed in backwards incompatible ways during the 1.x releases.

### Technical Preview

On occasion when a new feature is added to Kapacitor it may be added in a technical preview for a few minor releases and then later promoted to fully fledged v1 feature.
Preview means that the newly added features may be changed in backwards incompatible ways until they are promoted to v1 features.
Using technical preview allows for new features to fully mature while maintaining regularly scheduled releases.

To make it clear which features of the API are in technical preview the base path `/kapacitor/v1preview` is used.
If you wish to preview some of these new features, simply use the path `/kapacitor/v1preview` instead of `/kapacitor/v1` for your requests.
All v1 endpoints are available under the v1preview path so that your client need not be configured with multiple paths.
The technical preview endpoints are only available under the v1preview path.


>NOTE: Using a technical preview means that you may have to update your client for breaking changes to the previewed endpoints.

## Writing Data

Kapacitor can accept writes over HTTP using the line protocol.
This endpoint is identical in nature to the InfluxDB write endpoint.

| Query Parameter | Purpose                               |
| --------------- | -------                               |
| db              | Database name for the writes.         |
| rp              | Retention policy name for the writes. |

>NOTE: Kapacitor scopes all points by their database and retention policy.
This means you MUST specify the `rp` for writes or Kapacitor will not know which retention policy to use.

#### Example

Write data to Kapacitor.

```
POST /kapacitor/v1/write?db=DB_NAME&rp=RP_NAME
cpu,host=example.com value=87.6
```

For compatibility with the equivalent InfluxDB write endpoint the `/write` endpoint is maintained as an alias to the `/kapacitor/v1/write` endpoint.

```
POST /write?db=DB_NAME&rp=RP_NAME
cpu,host=example.com value=87.6
```

## Tasks

A task represents work for Kapacitor to perform.
A task is defined by its id, type, TICKscript, and list of database retention policy pairs it is allowed to access.

### Define Task

To define a task POST to the `/kapacitor/v1/tasks` endpoint.
If a task already exists then use the `PATCH` method to modify any property of the task.

Define a task using a JSON object with the following options:

| Property    | Purpose                                                                                   |
| --------    | -------                                                                                   |
| id          | Unique identifier for the task. If empty a random ID will be chosen.                      |
| template-id | An optional ID of a template to use instead of specifying a TICKscript and type directly. |
| type        | The task type: `stream` or `batch`.                                                       |
| dbrps       | List of database retention policy pairs the task is allowed to access.                    |
| script      | The content of the script.                                                                |
| status      | One of `enabled` or `disabled`.                                                           |
| vars        | A set of vars for overwriting any defined vars in the TICKscript.                         |

When using PATCH, if any option is missing it will be left unmodified.

##### Vars

The vars object has the form:

```json
{
    "field_name" : {
        "value": <VALUE>,
        "type": <TYPE>
    },
    "another_field" : {
        "value": <VALUE>,
        "type": <TYPE>
    }
}
```

The following is a table of valid types and example values.

| Type     | Example Value                    | Description                                                                                             |
| ----     | -------------                    | -----------                                                                                             |
| bool     | true                             | "true" or "false"                                                                                       |
| int      | 42                               | Any integer value                                                                                       |
| float    | 2.5 or 67                        | Any numeric value                                                                                       |
| duration | "1s" or 1000000000               | Any integer value interpretted in nanoseconds or an influxql duration string, (i.e. 10000000000 is 10s) |
| string   | "a string"                       | Any string value                                                                                        |
| regex    | "^abc.*xyz"                      | Any string value that represents a valid Go regular expression https://golang.org/pkg/regexp/           |
| lambda   | "\"value\" > 5"                  | Any string that is a valid TICKscript lambda expression                                                 |
| star     | ""                               | No value is required, a star type var represents the literal `*` in TICKscript (i.e. `.groupBy(*)`)     |
| list     | [{"type": TYPE, "value": VALUE}] | A list of var objects. Currently lists may only contain string or star vars                             |

#### Example

Create a new task with ID TASK_ID.

```
POST /kapacitor/v1/tasks
{
    "id" : "TASK_ID",
    "type" : "stream",
    "dbrps": [{"db": "DATABASE_NAME", "rp" : "RP_NAME"}],
    "script": "stream\n    |from()\n        .measurement('cpu')\n",
    "vars" : {
        "var1": {
            "value": 42,
            "type": "float"
        }
    }
}
```

Response with task id and link.

```json
{
    "link" : {"rel": "self", "href": "/kapacitor/v1/tasks/TASK_ID"},
    "id" : "TASK_ID",
    "type" : "stream",
    "dbrps" : [{"db": "DATABASE_NAME", "rp" : "RP_NAME"}],
    "script" : "stream\n    |from()\n        .measurement('cpu')\n",
    "dot" : "digraph TASK_ID { ... }",
    "vars" : {
        "var1": {
            "value": 42,
            "type": "float"
        }
    },
    "status" : "enabled",
    "executing" : true,
    "error" : "",
    "created": "2006-01-02T15:04:05Z07:00",
    "modified": "2006-01-02T15:04:05Z07:00",
    "stats" : {}
}
```

Modify only the dbrps of the task.

```
PATCH /kapacitor/v1/tasks/TASK_ID
{
    "dbrps": [{"db": "NEW_DATABASE_NAME", "rp" : "NEW_RP_NAME"}]
}
```

>NOTE: Setting any DBRP will overwrite all stored DBRPs.
Setting any Vars will overwrite all stored Vars.


Enable an existing task.

```
PATCH /kapacitor/v1/tasks/TASK_ID
{
    "status" : "enabled",
}
```

Disable an existing task.

```
PATCH /kapacitor/v1/tasks/TASK_ID
{
    "status" : "disabled",
}
```

Define a new task that is enabled on creation.

```
POST /kapacitor/v1/tasks
{
    "id" : "TASK_ID",
    "type" : "stream",
    "dbrps" : [{"db": "DATABASE_NAME", "rp" : "RP_NAME"}],
    "script" : "stream\n    |from()\n        .measurement('cpu')\n",
    "status" : "enabled"
}
```

Response with task id and link.

```json
{
    "id" : "TASK_ID",
    "link" : {"rel": "self", "href": "/kapacitor/v1/tasks/TASK_ID"}
}
```

#### Response

| Code | Meaning                                  |
| ---- | -------                                  |
| 200  | Task created, contains task information. |
| 404  | Task does not exist                      |

### Get Task

To get information about a task make a GET request to the `/kapacitor/v1/tasks/TASK_ID` endpoint.

| Query Parameter | Default    | Purpose                                                                                                                          |
| --------------- | -------    | -------                                                                                                                          |
| dot-view        | attributes | One of `labels` or `attributes`. Labels is less readable but will correctly render with all the information contained in labels. |
| script-format   | formatted  | One of `formatted` or `raw`. Raw will return the script identical to how it was defined. Formatted will first format the script. |
| replay-id       |            | Optional ID of a running replay. The returned task information will be in the context of the task for the running replay.        |


A task has these read only properties in addition to the properties listed [above](#define-task).

| Property     | Description                                                                                                                     |
| --------     | -----------                                                                                                                     |
| dot          | [GraphViz DOT](https://en.wikipedia.org/wiki/DOT_(graph_description_language)) syntax formatted representation of the task DAG. |
| executing    | Whether the task is currently executing.                                                                                        |
| error        | Any error encountered when executing the task.                                                                                  |
| stats        | Map of statistics about a task.                                                                                                 |
| created      | Date the task was first created                                                                                                 |
| modified     | Date the task was last modified                                                                                                 |
| last-enabled | Date the task was last set to status `enabled`                                                                                  |

#### Example

Get information about a task using defaults.

```
GET /kapacitor/v1/tasks/TASK_ID
```

```json
{
    "link" : {"rel": "self", "href": "/kapacitor/v1/tasks/TASK_ID"},
    "id" : "TASK_ID",
    "type" : "stream",
    "dbrps" : [{"db": "DATABASE_NAME", "rp" : "RP_NAME"}],
    "script" : "stream\n    |from()\n        .measurement('cpu')\n",
    "dot" : "digraph TASK_ID { ... }",
    "status" : "enabled",
    "executing" : true,
    "error" : "",
    "created": "2006-01-02T15:04:05Z07:00",
    "modified": "2006-01-02T15:04:05Z07:00",
    "last-enabled": "2006-01-03T15:04:05Z07:00",
    "stats" : {}
}
```

Get information about a task using only labels in the DOT content and skip the format step.

```
GET /kapacitor/v1/tasks/TASK_ID?dot-view=labels&script-format=raw
```

```json
{
    "link" : {"rel": "self", "href": "/kapacitor/v1/tasks/TASK_ID"},
    "id" : "TASK_ID",
    "type" : "stream",
    "dbrps" : [{"db": "DATABASE_NAME", "rp" : "RP_NAME"}],
    "script" : "stream|from().measurement('cpu')",
    "dot" : "digraph TASK_ID { ... }",
    "status" : "enabled",
    "executing" : true,
    "error" : "",
    "created": "2006-01-02T15:04:05Z07:00",
    "modified": "2006-01-02T15:04:05Z07:00",
    "last-enabled": "2006-01-03T15:04:05Z07:00",
    "stats" : {}
}
```

#### Response

| Code | Meaning             |
| ---- | -------             |
| 200  | Success             |
| 404  | Task does not exist |


### Delete Task

To delete a task make a DELETE request to the `/kapacitor/v1/tasks/TASK_ID` endpoint.

```
DELETE /kapacitor/v1/tasks/TASK_ID
```

#### Response

| Code | Meaning |
| ---- | ------- |
| 204  | Success |

>NOTE: Deleting a non-existent task is not an error and will return a 204 success.


### List Tasks

To get information about several tasks make a GET request to the `/kapacitor/v1/tasks` endpoint.

| Query Parameter | Default    | Purpose                                                                                                                                           |
| --------------- | -------    | -------                                                                                                                                           |
| pattern         |            | Filter results based on the pattern. Uses standard shell glob matching, see [this](https://golang.org/pkg/path/filepath/#Match) for more details. |
| fields          |            | List of fields to return. If empty returns all fields. Fields `id` and `link` are always returned.                                                |
| dot-view        | attributes | One of `labels` or `attributes`. Labels is less readable but will correctly render with all the information contained in labels.                  |
| script-format   | formatted  | One of `formatted` or `raw`. Raw will return the script identical to how it was defined. Formatted will first format the script.                  |
| offset          | 0          | Offset count for paginating through tasks.                                                                                                        |
| limit           | 100        | Maximum number of tasks to return.                                                                                                                |

#### Example

Get all tasks.

```
GET /kapacitor/v1/tasks
```

```json
{
    "tasks" : [
        {
            "link" : {"rel":"self", "href":"/kapacitor/v1/tasks/TASK_ID"},
            "id" : "TASK_ID",
            "type" : "stream",
            "dbrps" : [{"db": "DATABASE_NAME", "rp" : "RP_NAME"}],
            "script" : "stream|from().measurement('cpu')",
            "dot" : "digraph TASK_ID { ... }",
            "status" : "enabled",
            "executing" : true,
            "error" : "",
            "stats" : {}
        },
        {
            "link" : {"rel":"self", "href":"/kapacitor/v1/tasks/ANOTHER_TASK_ID"},
            "id" : "ANOTHER_TASK_ID",
            "type" : "stream",
            "dbrps" : [{"db": "DATABASE_NAME", "rp" : "RP_NAME"}],
            "script" : "stream|from().measurement('cpu')",
            "dot" : "digraph ANOTHER_TASK_ID{ ... }",
            "status" : "disabled",
            "executing" : true,
            "error" : "",
            "stats" : {}
        }
    ]
}
```

Optionally specify a glob `pattern` to list only matching tasks.

```
GET /kapacitor/v1/tasks?pattern=TASK*
```

```json
{
    "tasks" : [
        {
            "link" : {"rel":"self", "href":"/kapacitor/v1/tasks/TASK_ID"},
            "id" : "TASK_ID",
            "type" : "stream",
            "dbrps" : [{"db": "DATABASE_NAME", "rp" : "RP_NAME"}],
            "script" : "stream|from().measurement('cpu')",
            "dot" : "digraph TASK_ID { ... }",
            "status" : "enabled:,
            "executing" : true,
            "error" : "",
            "stats" : {}
        }
    ]
}
```

Get all tasks, but only the status, executing and error fields.

```
GET /kapacitor/v1/tasks?fields=status&fields=executing&fields=error
```

```json
{
    "tasks" : [
        {
            "link" : {"rel":"self", "href":"/kapacitor/v1/tasks/TASK_ID"},
            "id" : "TASK_ID",
            "status" : "enabled",
            "executing" : true,
            "error" : "",
        },
        {
            "link" : {"rel":"self", "href":"/kapacitor/v1/tasks/ANOTHER_TASK_ID"},
            "id" : "ANOTHER_TASK_ID",
            "status" : "disabled",
            "executing" : true,
            "error" : "",
        }
    ]
}
```

#### Response

| Code | Meaning |
| ---- | ------- |
| 200  | Success |

>NOTE: If the pattern does not match any tasks an empty list will be returned, with a 200 success.

### Custom Task HTTP Endpoints

In TICKscript it is possible to expose a cache of recent data via the [HTTPOut](https://docs.influxdata.com/kapacitor/latest/nodes/http_out_node/) node.
The data is available at the path `/kapacitor/v1/tasks/TASK_ID/ENDPOINT_NAME`.

### Example

For the TICKscript:

```go
stream
    |from()
        .measurement('cpu')
    |window()
        .period(60s)
        .every(60s)
    |httpOut('mycustom_endpoint')
```

```
GET /kapacitor/v1/tasks/TASK_ID/mycustom_endpoint
```

```json
{
    "series": [
        {
            "name": "cpu",
            "columns": [
                "time",
                "value"
            ],
            "values": [
                [
                    "2015-01-29T21:55:43.702900257Z",
                    55
                ],
                [
                    "2015-01-29T21:56:43.702900257Z",
                    42
                ],
            ]
        }
    ]
}
```

The output is the same as a query for data to [InfluxDB](https://docs.influxdata.com/influxdb/latest/guides/querying_data/).


## Templates

You can also define a task templates.
A task template is defined by a template TICKscript, and a task type.


### Define Templates

To define a template POST to the `/kapacitor/v1/templates` endpoint.
If a template already exists then use the `PATCH` method to modify any property of the template.

Define a template using a JSON object with the following options:

| Property | Purpose                                                                  |
| -------- | -------                                                                  |
| id       | Unique identifier for the template. If empty a random ID will be chosen. |
| type     | The template type: `stream` or `batch`.                                  |
| script   | The content of the script.                                               |

When using PATCH, if any option is missing it will be left unmodified.


#### Updating Templates

When updating an existing template all associated tasks are reloaded with the new template definition.
The first error if any is returned when reloading associated tasks.
If an error occurs, any task that was updated to the new definition is reverted to the old definition.
This ensures that all associated tasks for a template either succeed or fail together.

As a result, you will not be able to update a template if it introduces a breaking change in the TICKscript.
In order to update a template in a breaking way you have two options:

1. Create a new template and reassign each task to the new template updating the task vars as needed.
2. If the breaking change is forward compatible (i.e. adds a new required var), first update each task with the needed vars,
then update the template once all tasks are ready.


#### Example

Create a new template with ID TEMPLATE_ID.

```
POST /kapacitor/v1/templates
{
    "id" : "TEMPLATE_ID",
    "type" : "stream",
    "script": "stream\n    |from()\n        .measurement('cpu')\n"
}
```

Response with template id and link.

```json
{
    "link" : {"rel": "self", "href": "/kapacitor/v1/templates/TASK_ID"},
    "id" : "TASK_ID",
    "type" : "stream",
    "script" : "stream\n    |from()\n        .measurement('cpu')\n",
    "dot" : "digraph TASK_ID { ... }",
    "error" : "",
    "created": "2006-01-02T15:04:05Z07:00",
    "modified": "2006-01-02T15:04:05Z07:00",
}
```

Modify only the script of the template.

```
PATCH /kapacitor/v1/templates/TEMPLATE_ID
{
    "script": "stream|from().measurement('mem')"
}
```

#### Response

| Code | Meaning                                          |
| ---- | -------                                          |
| 200  | Template created, contains template information. |
| 404  | Template does not exist                          |

### Get Template

To get information about a template make a GET request to the `/kapacitor/v1/templates/TEMPLATE_ID` endpoint.

| Query Parameter | Default    | Purpose                                                                                                                          |
| --------------- | -------    | -------                                                                                                                          |
| script-format   | formatted  | One of `formatted` or `raw`. Raw will return the script identical to how it was defined. Formatted will first format the script. |


A template has these read only properties in addition to the properties listed [above](#define-template).

| Property | Description                                                                                                                                                                                                         |
| -------- | -----------                                                                                                                                                                                                         |
| vars     | Set of named vars from the TICKscript with their type, default values and description.                                                                                                                                           |
| dot      | [GraphViz DOT](https://en.wikipedia.org/wiki/DOT_(graph_description_language)) syntax formatted representation of the template DAG. NOTE: lables vs attributes does not matter since a template is never executing. |
| error    | Any error encountered when reading the template.                                                                                                                                                                    |
| created  | Date the template was first created                                                                                                                                                                                 |
| modified | Date the template was last modified                                                                                                                                                                                 |

#### Example

Get information about a template using defaults.

```
GET /kapacitor/v1/templates/TEMPLATE_ID
```

```json
{
    "link" : {"rel": "self", "href": "/kapacitor/v1/templates/TEMPLATE_ID"},
    "id" : "TASK_ID",
    "type" : "stream",
    "script" : "var x = 5\nstream\n    |from()\n        .measurement('cpu')\n",
    "vars": {"x":{"value": 5, "type":"int", "description": "threshold value"}},
    "dot" : "digraph TASK_ID { ... }",
    "error" : "",
    "created": "2006-01-02T15:04:05Z07:00",
    "modified": "2006-01-02T15:04:05Z07:00",
}
```

#### Response

| Code | Meaning                 |
| ---- | -------                 |
| 200  | Success                 |
| 404  | Template does not exist |


### Delete Template

To delete a template make a DELETE request to the `/kapacitor/v1/templates/TEMPLATE_ID` endpoint.

>NOTE:Deleting a template renders all associated tasks as orphans. The current state of the orphaned tasks will be left unmodified,
but orphaned tasks will not be able to be enabled.

```
DELETE /kapacitor/v1/templates/TEMPLATE_ID
```

#### Response

| Code | Meaning |
| ---- | ------- |
| 204  | Success |

>NOTE: Deleting a non-existent template is not an error and will return a 204 success.


### List Templates

To get information about several templates make a GET request to the `/kapacitor/v1/templates` endpoint.

| Query Parameter | Default    | Purpose                                                                                                                                           |
| --------------- | -------    | -------                                                                                                                                           |
| pattern         |            | Filter results based on the pattern. Uses standard shell glob matching, see [this](https://golang.org/pkg/path/filepath/#Match) for more details. |
| fields          |            | List of fields to return. If empty returns all fields. Fields `id` and `link` are always returned.                                                |
| script-format   | formatted  | One of `formatted` or `raw`. Raw will return the script identical to how it was defined. Formatted will first format the script.                  |
| offset          | 0          | Offset count for paginating through templates.                                                                                                        |
| limit           | 100        | Maximum number of templates to return.                                                                                                                |

#### Example

Get all templates.

```
GET /kapacitor/v1/templates
```

```json
{
    "templates" : [
        {
            "link" : {"rel":"self", "href":"/kapacitor/v1/templates/TEMPLATE_ID"},
            "id" : "TEMPLATE_ID",
            "type" : "stream",
            "script" : "stream|from().measurement('cpu')",
            "dot" : "digraph TEMPLATE_ID { ... }",
            "error" : ""
        },
        {
            "link" : {"rel":"self", "href":"/kapacitor/v1/templates/ANOTHER_TEMPLATE_ID"},
            "id" : "ANOTHER_TEMPLATE_ID",
            "type" : "stream",
            "script" : "stream|from().measurement('cpu')",
            "dot" : "digraph ANOTHER_TEMPLATE_ID{ ... }",
            "error" : ""
        }
    ]
}
```

Optionally specify a glob `pattern` to list only matching templates.

```
GET /kapacitor/v1/template?pattern=TEMPLATE*
```

```json
{
    "templates" : [
        {
            "link" : {"rel":"self", "href":"/kapacitor/v1/templates/TEMPLATE_ID"},
            "id" : "TEMPLATE_ID",
            "type" : "stream",
            "script" : "stream|from().measurement('cpu')",
            "dot" : "digraph TEMPLATE_ID { ... }",
            "error" : ""
        }
    ]
}
```

Get all templates, but only the script and error fields.

```
GET /kapacitor/v1/templates?fields=status&fields=executing&fields=error
```

```json
{
    "templates" : [
        {
            "link" : {"rel":"self", "href":"/kapacitor/v1/templates/TEMPLATE_ID"},
            "id" : "TEMPLATE_ID",
            "script" : "stream|from().measurement('cpu')",
            "error" : ""
        },
        {
            "link" : {"rel":"self", "href":"/kapacitor/v1/templates/ANOTHER_TEMPLATE_ID"},
            "id" : "ANOTHER_TEMPLATE_ID",
            "script" : "stream|from().measurement('cpu')",
            "error" : ""
        }
    ]
}
```

#### Response

| Code | Meaning |
| ---- | ------- |
| 200  | Success |

>NOTE: If the pattern does not match any templates an empty list will be returned, with a 200 success.


## Recordings

Kapacitor can save recordings of data and replay them against a specified task.

### Start Recording

There are three methods for recording data with Kapacitor:
To create a recording make a POST request to the `/kapacitor/v1/recordings/METHOD` endpoint.

| Method | Description                                        |
| ------ | -----------                                        |
| stream | Record the incoming stream of data.                |
| batch  | Record the results of the queries in a batch task. |
| query  | Record the result of an explicit query.            |

The request returns once the recording is started and does not wait for it to finish.
A recording ID is returned to later identify the recording.

##### Stream

| Parameter | Purpose                                                                    |
| --------- | -------                                                                    |
| id        | Unique identifier for the recording. If empty a random one will be chosen. |
| task      | ID of a task, used to only record data for the DBRPs of the task.          |
| stop      | Record stream data until stop date.                                        |

##### Batch

| Parameter | Purpose                                                                                                          |
| --------- | -------                                                                                                          |
| id        | Unique identifier for the recording. If empty a random one will be chosen.                                       |
| task      | ID of a task, records the results of the queries defined in the task.                                            |
| start     | Earliest date for which data will be recorded. RFC3339Nano formatted.                                            |
| stop      | Latest date for which data will be recorded. If not specified uses the current time. RFC3339Nano formatted data. |

##### Query

| Parameter | Purpose                                                                    |
| --------- | -------                                                                    |
| id        | Unique identifier for the recording. If empty a random one will be chosen. |
| type      | Type of recording, `stream` or `batch`.                                    |
| query     | Query to execute.                                                          |
| cluster   | Name of a configured InfluxDB cluster. If empty uses the default cluster.  |

>NOTE: A recording itself is typed as either a stream or batch recording and can only be replayed to a task of a corresponding type.
Therefore when you record the result of a raw query you must specify the type recording you wish to create.


#### Example

Create a recording using the `stream` method

```
POST /kapacitor/v1/recordings/stream
{
    "task" : "TASK_ID",
    "stop" : "2006-01-02T15:04:05Z07:00"
}
```

Create a recording using the `batch` method specifying a start time.

```
POST /kapacitor/v1/recordings/batch
{
    "task" : "TASK_ID",
    "start" : "2006-01-02T15:04:05Z07:00"
}
```

Create a recording using the `query` method specifying a `stream` type.

```
POST /kapacitor/v1/recordings/query
{
    "query" : "SELECT mean(usage_idle) FROM cpu WHERE time > now() - 1h GROUP BY time(10m)",
    "type" : "stream"
}
```

Create a recording using the `query` method specifying a `batch` type.

```
POST /kapacitor/v1/recordings/query
{
    "query" : "SELECT mean(usage_idle) FROM cpu WHERE time > now() - 1h GROUP BY time(10m)",
    "type" : "batch"
}
```

Create a recording with a custom ID.

```
POST /kapacitor/v1/recordings/query
{
    "id" : "MY_RECORDING_ID",
    "query" : "SELECT mean(usage_idle) FROM cpu WHERE time > now() - 1h GROUP BY time(10m)",
    "type" : "batch"
}
```

#### Response

All recordings are assigned an ID which is returned in this format with a link.

```json
{
    "link" : {"rel": "self", "href": "/kapacitor/v1/recordings/e24db07d-1646-4bb3-a445-828f5049bea0"},
    "id" : "e24db07d-1646-4bb3-a445-828f5049bea0",
    "type" : "stream",
    "size" : 0,
    "date" : "2006-01-02T15:04:05Z07:00",
    "error" : "",
    "status" : "running",
    "progress" : 0
}
```

| Code | Meaning                             |
| ---- | -------                             |
| 201  | Success, the recording has started. |

### Wait for Recording

In order to determine when a recording has finished you must make a GET request to the returned link typically something like `/kapacitor/v1/recordings/RECORDING_ID`.

A recording has these read only properties.

| Property | Description                                                                  |
| -------- | -----------                                                                  |
| size     | Size of the recording on disk in bytes.                                      |
| date     | Date the recording finished.                                                 |
| error    | Any error encountered when creating the recording.                           |
| status   | One of `recording` or `finished`.                                            |
| progress | Number between 0 and 1 indicating the approximate progress of the recording. |


#### Example

```
GET /kapacitor/v1/recordings/e24db07d-1646-4bb3-a445-828f5049bea0
```

```json
{
    "link" : {"rel": "self", "href": "/kapacitor/v1/recordings/e24db07d-1646-4bb3-a445-828f5049bea0"},
    "id" : "e24db07d-1646-4bb3-a445-828f5049bea0",
    "type" : "stream",
    "size" : 1980353,
    "date" : "2006-01-02T15:04:05Z07:00",
    "error" : "",
    "status" : "running",
    "progress" : 0.75
}
```

Once the recording is complete.

```
GET /kapacitor/v1/recordings/e24db07d-1646-4bb3-a445-828f5049bea0
```

```json
{
    "link" : {"rel": "self", "href": "/kapacitor/v1/recordings/e24db07d-1646-4bb3-a445-828f5049bea0"},
    "id" : "e24db07d-1646-4bb3-a445-828f5049bea0",
    "type" : "stream",
    "size" : 1980353,
    "date" : "2006-01-02T15:04:05Z07:00",
    "error" : "",
    "status" : "finished",
    "progress" : 1
}
```

Or if the recording fails.

```
GET /kapacitor/v1/recordings/e24db07d-1646-4bb3-a445-828f5049bea0
```

```json
{
    "link" : {"rel": "self", "href": "/kapacitor/v1/recordings/e24db07d-1646-4bb3-a445-828f5049bea0"},
    "id" : "e24db07d-1646-4bb3-a445-828f5049bea0",
    "type" : "stream",
    "size" : 1980353,
    "date" : "2006-01-02T15:04:05Z07:00",
    "error" : "error message explaining failure",
    "status" : "failed",
    "progress" : 1
}
```

#### Response

| Code | Meaning                                            |
| ---- | -------                                            |
| 200  | Success, the recording is no longer running.       |
| 202  | Success, the recording exists but is not finished. |
| 404  | No such recording exists.                          |

### Delete Recording

To delete a recording make a DELETE request to the `/kapacitor/v1/recordings/RECORDING_ID` endpoint.

```
DELETE /kapacitor/v1/recordings/RECORDING_ID
```

#### Response

| Code | Meaning |
| ---- | ------- |
| 204  | Success |

>NOTE: Deleting a non-existent recording is not an error and will return a 204 success.

### List Recordings

To list all recordings make a GET request to the `/kapacitor/v1/recordings` endpoint.
Recordings are sorted by date.

| Query Parameter | Default | Purpose                                                                                                                                           |
| --------------- | ------- | -------                                                                                                                                           |
| pattern         |         | Filter results based on the pattern. Uses standard shell glob matching, see [this](https://golang.org/pkg/path/filepath/#Match) for more details. |
| fields          |         | List of fields to return. If empty returns all fields. Fields `id` and `link` are always returned.                                                |
| offset          | 0       | Offset count for paginating through tasks.                                                                                                        |
| limit           | 100     | Maximum number of tasks to return.                                                                                                                |

#### Example

```
GET /kapacitor/v1/recordings
```

```json
{
    "recordings" : [
        {
            "link" : {"rel": "self", "href": "/kapacitor/v1/recordings/e24db07d-1646-4bb3-a445-828f5049bea0"},
            "id" : "e24db07d-1646-4bb3-a445-828f5049bea0",
            "type" : "stream",
            "size" : 1980353,
            "date" : "2006-01-02T15:04:05Z07:00",
            "error" : "",
            "status" : "finished",
            "progress" : 1
        },
        {
            "link" : {"rel": "self", "href": "/kapacitor/v1/recordings/8a4c06c6-30fb-42f4-ac4a-808aa31278f6"},
            "id" : "8a4c06c6-30fb-42f4-ac4a-808aa31278f6",
            "type" : "batch",
            "size" : 216819562,
            "date" : "2006-01-02T15:04:05Z07:00",
            "error" : "",
            "status" : "finished",
            "progress" : 1
        }
    ]
}
```

#### Response

| Code | Meaning |
| ---- | ------- |
| 200  | Success |


## Replays

### Replaying a recording

To replay a recording make a POST request to `/kapacitor/v1/replays/`

| Parameter      | Default | Purpose                                                                                                                                                                                                                                          |
| ----------     | ------- | -------                                                                                                                                                                                                                                          |
| id             | random  | Unique identifier for the replay. If empty a random ID is chosen.                                                                                                                                                                                |
| task           |         | ID of task.                                                                                                                                                                                                                                      |
| recording      |         | ID of recording.                                                                                                                                                                                                                                 |
| recording-time | false   | If true, use the times in the recording, otherwise adjust times relative to the current time.                                                                                                                                                    |
| clock          | fast    | One of `fast` or `real`. If `real` wait for real time to pass corresponding with the time in the recordings. If `fast` replay data without delay. For example, if clock is `real` then a stream recording of duration 5m will take 5m to replay. |

#### Example

Replay a recording using default parameters.

```
POST /kapacitor/v1/replays/
{
    "task" : "TASK_ID",
    "recording" : "RECORDING_ID"
}
```

Replay a recording in real-time mode and preserve recording times.

```
POST /kapacitor/v1/replays/
{
    "task" : "TASK_ID",
    "recording" : "RECORDING_ID",
    "clock" : "real",
    "recording-time" : true,
}
```

Replay a recording using a custom ID.

```
POST /kapacitor/v1/replays/
{
    "id" : "MY_REPLAY_ID",
    "task" : "TASK_ID",
    "recording" : "RECORDING_ID"
}
```

#### Response

The request returns once the replay is started and provides a replay ID and link.

```json
{
    "link" : {"rel": "self", "href": "/kapacitor/v1/replays/ad95677b-096b-40c8-82a8-912706f41d4c"},
    "id" : "ad95677b-096b-40c8-82a8-912706f41d4c",
    "task" : "TASK_ID",
    "recording" : "RECORDING_ID",
    "clock" : "fast",
    "recording-time" : false,
    "status" : "running",
    "progress" : 0,
    "error" : ""
}
```

| Code | Meaning                      |
| ---- | -------                      |
| 201  | Success, replay has started. |

### Replay data without Recording

It is also possible to replay data directly without recording it first.
This is done by issuing a request similar to either a `batch` or `query` recording
but instead of storing the data it is immediately replayed against a task.
Using a `stream` recording for immediately replaying against a task is equivalent to enabling the task
and so is not supported.

| Method | Description                                        |
| ------ | -----------                                        |
| batch  | Replay the results of the queries in a batch task. |
| query  | Replay the results of an explicit query.           |


##### Batch

| Parameter      | Default | Purpose                                                                                                                                                                                                                                          |
| ---------      | ------- | -------                                                                                                                                                                                                                                          |
| id             | random  | Unique identifier for the replay. If empty a random one will be chosen.                                                                                                                                                                          |
| task           |         | ID of a task, replays the results of the queries defined in the task against the task.                                                                                                                                                                            |
| start          |         | Earliest date for which data will be replayed. RFC3339Nano formatted.                                                                                                                                                                            |
| stop           | now     | Latest date for which data will be replayed. If not specified uses the current time. RFC3339Nano formatted data.                                                                                                                                 |
| recording-time | false   | If true, use the times in the recording, otherwise adjust times relative to the current time.                                                                                                                                                    |
| clock          | fast    | One of `fast` or `real`. If `real` wait for real time to pass corresponding with the time in the recordings. If `fast` replay data without delay. For example, if clock is `real` then a stream recording of duration 5m will take 5m to replay. |

##### Query

| Parameter      | Default | Purpose                                                                                                                                                                                                                                          |
| ---------      | ------- | -------                                                                                                                                                                                                                                          |
| id             | random  | Unique identifier for the replay. If empty a random one will be chosen.                                                                                                                                                                          |
| task           |         | ID of a task, replays the results of the queries against the task.                                                                                                                                                                               |
| query          |         | Query to execute.                                                                                                                                                                                                                                |
| cluster        |         | Name of a configured InfluxDB cluster. If empty uses the default cluster.                                                                                                                                                                        |
| recording-time | false   | If true, use the times in the recording, otherwise adjust times relative to the current time.                                                                                                                                                    |
| clock          | fast    | One of `fast` or `real`. If `real` wait for real time to pass corresponding with the time in the recordings. If `fast` replay data without delay. For example, if clock is `real` then a stream recording of duration 5m will take 5m to replay. |

#### Example

Perform a replay using the `batch` method specifying a start time.

```
POST /kapacitor/v1/replays/batch
{
    "task" : "TASK_ID",
    "start" : "2006-01-02T15:04:05Z07:00"
}
```

Replay the results of the query against the task.

```
POST /kapacitor/v1/replays/query
{
    "task" : "TASK_ID",
    "query" : "SELECT mean(usage_idle) FROM cpu WHERE time > now() - 1h GROUP BY time(10m)",
}
```

Create a replay with a custom ID.

```
POST /kapacitor/v1/replays/query
{
    "id" : "MY_REPLAY_ID",
    "task" : "TASK_ID",
    "query" : "SELECT mean(usage_idle) FROM cpu WHERE time > now() - 1h GROUP BY time(10m)",
}
```

#### Response

All replays are assigned an ID which is returned in this format with a link.

```json
{
    "link" : {"rel": "self", "href": "/kapacitor/v1/replays/e24db07d-1646-4bb3-a445-828f5049bea0"},
    "id" : "e24db07d-1646-4bb3-a445-828f5049bea0",
    "task" : "TASK_ID",
    "recording" : "",
    "clock" : "fast",
    "recording-time" : false,
    "status" : "running",
    "progress" : 0.57,
    "error" : ""
}
```

>NOTE: For a replay created in this manner the `recording` ID will be empty since no recording was used or created.


| Code | Meaning                          |
| ---- | -------                          |
| 201  | Success, the replay has started. |


### Waiting for a Replay

Like recordings you make a GET request to the `/kapacitor/v1/replays/REPLAY_ID` endpoint to get the status of the replay.

A replay has these read only properties in addition to the properties listed [above](#replay-recording).

| Property | Description                                                               |
| -------- | -----------                                                               |
| status   | One of `replaying` or `finished`.                                           |
| progress | Number between 0 and 1 indicating the approximate progress of the replay. |
| error    | Any error that occured while perfoming the replay                         |


#### Example

Get the status of a replay.

```
GET /kapacitor/v1/replays/ad95677b-096b-40c8-82a8-912706f41d4c
```

```json
{
    "link" : {"rel": "self", "href": "/kapacitor/v1/replays/ad95677b-096b-40c8-82a8-912706f41d4c"},
    "id" : "ad95677b-096b-40c8-82a8-912706f41d4c",
    "task" : "TASK_ID",
    "recording" : "RECORDING_ID",
    "clock" : "fast",
    "recording-time" : false,
    "status" : "running",
    "progress" : 0.57,
    "error" : ""
}
```

Once the replay is complete.

```
GET /kapacitor/v1/replays/ad95677b-096b-40c8-82a8-912706f41d4c
```

```json
{
    "link" : {"rel": "self", "href": "/kapacitor/v1/replays/ad95677b-096b-40c8-82a8-912706f41d4c"},
    "id" : "ad95677b-096b-40c8-82a8-912706f41d4c",
    "task" : "TASK_ID",
    "recording" : "RECORDING_ID",
    "clock" : "fast",
    "recording-time" : false,
    "status" : "finished",
    "progress" : 1,
    "error" : ""
}
```

Or if the replay fails.

```
GET /kapacitor/v1/replays/ad95677b-096b-40c8-82a8-912706f41d4c
```

```json
{
    "link" : {"rel": "self", "href": "/kapacitor/v1/replays/ad95677b-096b-40c8-82a8-912706f41d4c"},
    "id" : "ad95677b-096b-40c8-82a8-912706f41d4c",
    "task" : "TASK_ID",
    "recording" : "RECORDING_ID",
    "clock" : "fast",
    "recording-time" : false,
    "status" : "failed",
    "progress" : 1,
    "error" : "error message explaining failure"
}
```

#### Response

| Code | Meaning                                         |
| ---- | -------                                         |
| 200  | Success, replay is no longer running.           |
| 202  | Success, the replay exists but is not finished. |
| 404  | No such replay exists.                          |

### Delete Replay

To delete a replay make a DELETE request to the `/kapacitor/v1/replays/REPLAY_ID` endpoint.

```
DELETE /kapacitor/v1/replays/REPLAY_ID
```

#### Response

| Code | Meaning |
| ---- | ------- |
| 204  | Success |

>NOTE: Deleting a non-existent replay is not an error and will return a 204 success.


### List Replays

You can list replays for a given recording by making a GET request to `/kapacitor/v1/replays`.

| Query Parameter | Default | Purpose                                                                                                                                           |
| --------------- | ------- | -------                                                                                                                                           |
| pattern         |         | Filter results based on the pattern. Uses standard shell glob matching, see [this](https://golang.org/pkg/path/filepath/#Match) for more details. |
| fields          |         | List of fields to return. If empty returns all fields. Fields `id` and `link` are always returned.                                                |
| offset          | 0       | Offset count for paginating through tasks.                                                                                                        |
| limit           | 100     | Maximum number of tasks to return.                                                                                                                |

#### Example

```
GET /kapacitor/v1/replays
```

```json
{
    "replays": [
        {
            "link" : {"rel": "self", "href": "/kapacitor/v1/replays/ad95677b-096b-40c8-82a8-912706f41d4c"},
            "id" : "ad95677b-096b-40c8-82a8-912706f41d4c",
            "task" : "TASK_ID",
            "recording" : "RECORDING_ID",
            "clock" : "fast",
            "recording-time" : false,
            "status" : "finished",
            "progress" : 1,
            "error" : ""
        },
        {
            "link" : {"rel": "self", "href": "/kapacitor/v1/replays/be33f0a1-0272-4019-8662-c730706dac7d"},
            "id" : "be33f0a1-0272-4019-8662-c730706dac7d",
            "task" : "TASK_ID",
            "recording" : "RECORDING_ID",
            "clock" : "fast",
            "recording-time" : false,
            "status" : "finished",
            "progress" : 1,
            "error" : ""
        }
    ]
}
```

## Alerts

Kapacitor can generate and handle alerts.
The API allows you to see the current state of any alert and to configure various handlers for the alerts.

>NOTE: All API endpoints related to alerts are in a technical preview.
Meaning that they are subject to change in the future until the technical preview is completed.
As such the URL for the endpoints uses the base path `/kapacitor/v1preview`.
Once the technical preview is deemed complete the endpoint paths will be promoted to use the v1 `/kapacitor/v1` base path.

### Topics

Alerts are grouped into topics.
An alert handler "listens" on a topic for any new events.
You can either specify the alert topic in the TICKscript or one will be generated for you.

To query the list of available topics make a GET requests to `/kapacitor/v1preview/alerts/topics`.

| Query Parameter | Default | Purpose                                                                                                                                                            |
| --------------- | ------- | -------                                                                                                                                                            |
| min-level       | OK      | Only return topics that are greater or equal to the min-level. Valid values include OK, INFO, WARNING, CRITICAL.                                                   |
| pattern         | *       | Filter results based on the pattern. Uses standard shell glob matching on the topic ID, see [this](https://golang.org/pkg/path/filepath/#Match) for more details. |


#### Example

Get all topics.

```
GET /kapacitor/v1preview/alerts/topics
```

```
{
    "link": {"rel":"self","href":"/kapacitor/v1preview/alerts/topics"},
    "topics": [
        {
            "link": {"rel":"self","href":"/kapacitor/v1preview/alerts/topics/system"},
            "events-link" : {"rel":"events","href":"/kapacitor/v1preview/alerts/topics/system/events"},
            "handlers-link": {"rel":"handlers","href":"/kapacitor/v1preview/alerts/topics/system/handlers"},
            "id": "system",
            "level":"CRITICAL"
        },
        {
            "link": {"rel":"self","href":"/kapacitor/v1preview/alerts/topics/app"},
            "events-link" : {"rel":"events","href":"/kapacitor/v1preview/alerts/topics/app/events"},
            "handlers-link": {"rel":"handlers","href":"/kapacitor/v1preview/alerts/topics/app/handlers"},
            "id": "app",
            "level":"OK"
        }
    ]
}
```

Get all topics in a WARNING or CRITICAL state.


```
GET /kapacitor/v1preview/alerts/topics?min-level=WARNING
```

```
{
    "link": {"rel":"self","href":"/kapacitor/v1preview/alerts/topics"},
    "topics": [
        {
            "link": {"rel":"self","href":"/kapacitor/v1preview/alerts/topics/system"},
            "events-link" : {"rel":"events","href":"/kapacitor/v1preview/alerts/topics/system/events"},
            "handlers-link": {"rel":"handlers","href":"/kapacitor/v1preview/alerts/topics/system/handlers"},
            "id": "system",
            "level":"CRITICAL"
        }
    ]
}
```

### Topic Status

To query the status of a topic make a GET request to `/kapacitor/v1preview/alerts/topics/<topic id>`.

#### Example

```
GET /kapacitor/v1preview/alerts/topics/system
```

```
{
    "link": {"rel":"self","href":"/kapacitor/v1preview/alerts/topics/system"},
    "id": "system",
    "level":"CRITICAL"
    "events-link" : {"rel":"events","href":"/kapacitor/v1preview/alerts/topics/system/events"},
    "handlers-link": {"rel":"handlers","href":"/kapacitor/v1preview/alerts/topics/system/handlers"},
}
```

### All Topic Events

To query all the events within a topic make a GET request to `/kapacitor/v1preview/alerts/topics/<topic id>/events`.

| Query Parameter | Default | Purpose                                                                                                          |
| --------------- | ------- | -------                                                                                                          |
| min-level       | OK      | Only return events that are greater or equal to the min-level. Valid values include OK, INFO, WARNING, CRITICAL. |

#### Example

```
GET /kapacitor/v1preview/alerts/topics/system/events
```

```
{
    "link": {"rel":"self","href":"/kapacitor/v1preview/alerts/topics/system/events"},
    "topic": "system",
    "events": [
        {
            "link":{"rel":"self","href":"/kapacitor/v1preview/alerts/topics/system/events/cpu"},
            "id": "cpu",
            "state": {
                "level": "WARNING",
                "message": "cpu is WARNING",
                "time": "2016-12-01T00:00:00Z",
                "duration": "5m"
            }
        },
        {
            "link":{"rel":"self","href":"/kapacitor/v1preview/alerts/topics/system/events/mem"},
            "id": "mem",
            "state": {
                "level": "CRITICAL",
                "message": "mem is CRITICAL",
                "time": "2016-12-01T00:10:00Z",
                "duration": "1m"
            }
        }
    ]
}
```

### Specific Topic Event

You can query a specific event within a topic by making a GET request to `/kapacitor/v1preview/alerts/topics/<topic id>/events/<event id>`.

#### Example

```
GET /kapacitor/v1preview/alerts/topics/system/events/cpu
```

```
{
    "link":{"rel":"self","href":"/kapacitor/v1preview/alerts/topics/system/events/cpu"},
    "id": "cpu",
    "state": {
        "level": "WARNING",
        "message": "cpu is WARNING",
        "time": "2016-12-01T00:00:00Z",
        "duration": "5m"
    }
}
```

### Topic Handlers

Handlers are created independent of a topic but are associated with a topic.
You can get a list of handlers configured for a topic by making a GET request to `/kapacitor/v1preview/alerts/topics/<topic id>/handlers`.

>NOTE: Anonymous handlers (created automatically from TICKscripts) will not be listed under their associated anonymous topic as they are not configured via the API.

#### Example


Get the handlers for the `system` topic.

```
GET /kapacitor/v1preview/alerts/topics/system/handlers
```

```
{
    "link":{"rel":"self","href":"/kapacitor/v1preview/alerts/topics/system/handlers"},
    "topic": "system",
    "handlers": [
        {
            "link":{"rel":"self","href":"/kapacitor/v1preview/alerts/handlers/slack"},
            "id":"slack",
            "topics": ["system", "app"],
            "actions": [{
                "kind":"slack",
                "options":{
                    "channel":"#alerts"
                }
            }]
        },
        {
            "link":{"rel":"self","href":"/kapacitor/v1preview/alerts/handlers/smtp"},
            "id":"smtp",
            "topics": ["system", "app"],
            "actions": [{
                "kind":"smtp"
            }]
        }
    ]
}
```

This `main:alert_cpu:alert5` topic represents an auto-generated topic from a task that has defined handlers explicitly in the TICKscript.
Anonymous handlers cannot be listed or modified via the API.

```
GET /kapacitor/v1preview/alerts/topics/main:alert_cpu:alert5/handlers
```

```
{
    "link":{"rel":"self","href":"/kapacitor/v1preview/alerts/topics/system/handlers"},
    "topic": "main:alert_cpu:alert5",
    "handlers": null
}
```

### Creating and Removing Topics

Topics are created dynamically for you when they referenced in TICKscripts or in handlers.
To delete a topic make a `DELETE` request to `/kapacitor/v1preview/alerts/topics/<topic id>`.
This will delete all known events and state for the topic.

>NOTE: Since topics are dynamically created, a topic may return after having deleted it, if a new event is created for the topic.


#### Example

```
DELETE /kapacitor/v1preview/alerts/topics/system
```


### List Handlers

To query information about all handlers independent of a given topic make a GET request to `/kapacitor/v1preview/alerts/handlers`.

| Query Parameter | Default | Purpose                                                                                                                                                               |
| --------------- | ------- | -------                                                                                                                                                               |
| pattern         | *       | Filter results based on the pattern. Uses standard shell glob matching on the service name, see [this](https://golang.org/pkg/path/filepath/#Match) for more details. |

#### Example

```
GET /kapacitor/v1preview/alerts/handlers
```

```
{
    "link":{"rel":"self","href":"/kapacitor/v1preview/alerts/handlers"},
    "handlers": [
        {
            "link":{"rel":"self","href":"/kapacitor/v1preview/alerts/handlers/slack"},
            "id":"slack",
            "topics": ["system", "app"],
            "actions": [{
                "kind":"slack",
                "options": {
                    "channel":"#alerts"
                }
            }]
        },
        {
            "link":{"rel":"self","href":"/kapacitor/v1preview/alerts/handlers/smtp"},
            "id":"smtp",
            "topics": ["system", "app"],
            "actions": [{
                "kind":"smtp"
            }]
        }
    ]
}
```

### Get a Handler

To query information about a specific handler make a GET request to `/kapacitor/v1preview/alerts/handlers/<handler id>`.

#### Example

```
GET /kapacitor/v1preview/alerts/handlers/slack
```

```
{
    "link":{"rel":"self","href":"/kapacitor/v1preview/alerts/handlers/slack"},
    "id":"slack",
    "topics": ["system", "app"],
    "actions": [{
        "kind":"slack",
        "options": {
            "channel":"#alerts"
        }
    }]
}
```

### Create a Handler

To create a new handler make a POST request to `/kapacitor/v1preview/alerts/handlers`.

```
POST /kapacitor/v1preview/alerts/handlers
{
    "id":"slack",
    "topics": ["system", "app"],
    "actions": [{
        "kind":"slack",
        "options": {
            "channel":"#alerts"
        }
    }]

}
```

```
{
    "link":{"rel":"self","href":"/kapacitor/v1preview/alerts/handlers/slack"},
    "id": "slack",
    "topics": ["system", "app"],
    "actions": [{
        "kind":"slack",
        "options": {
            "channel":"#alerts"
        }
    }]
}
```

### Update a Handler

To update an existing handler you can either make a PUT or PATCH request to `/kapacitor/v1preview/alerts/handlers/<handler id>`.

Using PUT will replace the entire handler, by using PATCH specific parts of the handler can be modified.

PATCH will apply JSON patch object to the existing handler, see [rfc6902](https://tools.ietf.org/html/rfc6902) for more details.

#### Example

Update the topics and actions for a handler using the PATCH method.

```
PATCH /kapacitor/v1preview/alerts/handlers/slack
[
    {"op":"replace", "path":"/topics", "value":["system", "test"]},
    {"op":"replace", "path":"/actions/0/options/channel", "value":"#testing_alerts"}
]
```

```
{
    "link":{"rel":"self","href":"/kapacitor/v1preview/alerts/handlers/slack"},
    "id": "slack",
    "topics": ["system", "test"],
    "actions": [
        {
            "kind":"slack",
            "options": {
                "channel":"#testing_alerts"
            }
        }
    ]
}
```

Replace an entire handler using the PUT method.

```
PUT /kapacitor/v1preview/alerts/handlers/slack
{
    "id": "slack",
    "topics": ["system", "test"],
    "actions": [
        {
            "kind":"slack",
            "options": {
                "channel":"#testing_alerts"
            }
        }
    ]
}
```

```
{
    "link":{"rel":"self","href":"/kapacitor/v1preview/alerts/handlers/slack"},
    "id": "slack",
    "topics": ["system", "test"],
    "actions": [
        {
            "kind":"slack",
            "options": {
                "channel":"#testing_alerts"
            }
        }
    ]
}
```

### Remove a Handler

To remove an existing handler make a DELETE request to `/kapacitor/v1preview/alerts/handlers/<handler id>`.

```
DELETE /kapacitor/v1preview/alerts/handlers/<handler id>
```

## Configuration

You can set configuration overrides via the API for certain sections of the config.
The overrides set via the API always take precedent over what may exist in the configuration file.
The sections available for overriding include the InfluxDB clusters and the alert handler sections.


The intent of the API is to allow for dynamic configuration of sensitive credentials without requiring that the Kapacitor process be restarted.
As such, it is recommended to use either the configuration file or the API to manage these configuration sections, but not both.
This will help to eliminate any confusion that may arise as to the source of a given configuration option.


### Enabling/Disabling Configuration Overrides

By default the ability to override the configuration is enabled.
If you do not wish to enable this feature it can be disabled via the `config-override` configuration section.

```
[config-override]
  enabled = false
```

If the `config-override` service is disabled then the relevant API endpoints will return 403 forbidden errors.

### Recovering from bad configuration

If somehow you have created a configuration that causes Kapacitor to crash or otherwise not function,
you can disable applying overrides during startup with the `skip-config-overrides` top level configuration option.

```
# This configuration option is only a safe guard and should not be needed in practice.
skip-config-overrides = true
```

This allows you to still access the API to fix any unwanted configuration without applying that configuration during statup.

>NOTE: It is probably easiest and safest to set this option as an environment variable `KAPACITOR_SKIP_CONFIG_OVERRIDES=true`, since it is meant to be temporary.
That way you do not have to modify your on disk configuration file or accidentally leave it in place causing issues later on.

### Overview

The paths for the configuration API endpoints are as follows:

`/kapacitor/v1/config/<section name>/[<element name>]`

Example:

```
/kapacitor/v1/config/smtp/
/kapacitor/v1/config/influxdb/localhost
/kapacitor/v1/config/influxdb/remote
```

The optional `element name` path element corresponds to a specific item from a list of entries.

For example the above paths correspond to the following configuration sections:

```
[smtp]
    # SMTP config here

[[influxdb]]
    name = "localhost"
    # InfluxDB config here for the "localhost" cluster

[[influxdb]]
    name = "remote"
    # InfluxDB config here for the "remote" cluster
```


### Retrieving the current configuration

To retrieve the current configuration perform a GET request to the desired path.
The returned configuration will be the merged values from the configuration file and what has been stored in the overrides.
The returned content will be JSON encoded version of the configuration objects.

All sensitive information will not be returned in the request body.
Instead a boolean value will be in its place indicating whether the value is empty or not.
A list of which options are redacted is returned for each element.

#### Example

Retrieve all the configuration sections which can be overridden.

```
GET /kapacitor/v1/config
```

```json
{
    "link" : {"rel": "self", "href": "/kapacitor/v1/config"},
    "sections": {
        "influxdb": {
            "link" : {"rel": "self", "href": "/kapacitor/v1/config/influxdb"},
            "elements": [
                {
                    "link" : {"rel": "self", "href": "/kapacitor/v1/config/influxdb/localhost"},
                    "options": {
                        "name": "localhost",
                        "urls": ["http://localhost:8086"],
                        "default": true,
                        "username": "",
                        "password": false
                    },
                    "redacted" : [
                        "password"
                    ]
                },
                {
                    "link" : {"rel": "self", "href": "/kapacitor/v1/config/influxdb/remote"},
                    "options": {
                        "name": "remote",
                        "urls": ["http://influxdb.example.com:8086"],
                        "default": false,
                        "username": "jim",
                        "password": true
                    },
                    "redacted" : [
                        "password"
                    ]
                }
            ]
        },
        "smtp": {
            "link" : {"rel": "self", "href": "/kapacitor/v1/config/smtp"},
            "elements": [{
                "link" : {"rel": "self", "href": "/kapacitor/v1/config/smtp/"},
                "options": {
                    "enabled": true,
                    "host": "smtp.example.com",
                    "port": 587,
                    "username": "bob",
                    "password": true,
                    "no-verify": false,
                    "global": false,
                    "to": [ "oncall@example.com"],
                    "from": "kapacitor@example.com",
                    "idle-timeout": "30s"
                },
                "redacted" : [
                    "password"
                ]
            }]
        }
    }
}
```


Retrieve only the SMTP section.

```
GET /kapacitor/v1/config/smtp
```

```json
{
    "link" : {"rel": "self", "href": "/kapacitor/v1/config/smtp"},
    "elements": [{
        "link" : {"rel": "self", "href": "/kapacitor/v1/config/smtp/"},
        "options": {
            "enabled": true,
            "host": "smtp.example.com",
            "port": 587,
            "username": "bob",
            "password": true,
            "no-verify": false,
            "global": false,
            "to": ["oncall@example.com"],
            "from": "kapacitor@example.com",
            "idle-timeout": "30s"
        },
        "redacted" : [
            "password"
        ]
    }]
}
```

Retrieve the single element from the SMTP section.

```
GET /kapacitor/v1/config/smtp/
```

```json
{
    "link" : {"rel": "self", "href": "/kapacitor/v1/config/smtp/"},
    "options": {
        "enabled": true,
        "host": "smtp.example.com",
        "port": 587,
        "username": "bob",
        "password": true,
        "no-verify": false,
        "global": false,
        "to": ["oncall@example.com"],
        "from": "kapacitor@example.com",
        "idle-timeout": "30s"
    },
    "redacted" : [
        "password"
    ]
}
```

>NOTE: Sections that are not lists can be treated as having an empty string for their element name.

Retrieve only the InfluxDB section.

```
GET /kapacitor/v1/config/influxdb
```

```json
{
    "link" : {"rel": "self", "href": "/kapacitor/v1/config/influxdb"},
    "elements" : [
        {
            "link" : {"rel": "self", "href": "/kapacitor/v1/config/influxdb/localhost"},
            "options": {
               "name": "localhost",
               "urls": ["http://localhost:8086"],
               "default": true,
               "username": "",
               "password": false
            },
            "redacted" : [
                "password"
            ]
        },
        {
            "link" : {"rel": "self", "href": "/kapacitor/v1/config/influxdb/remote"},
            "options": {
                "name": "remote",
                "urls": ["http://influxdb.example.com:8086"],
                "default": false,
                "username": "jim",
                "password": true
            },
            "redacted" : [
                "password"
            ]
        }
    ]
}
```

Retrieve only the `remote` element of the InfluxDB section.

```
GET /kapacitor/v1/config/influxdb/remote
```

```json
{
    "link" : {"rel": "self", "href": "/kapacitor/v1/config/influxdb/remote"},
    "options": {
        "name": "remote",
        "urls": ["http://influxdb.example.com:8086"],
        "default": false,
        "username": "jim",
        "password": true
    },
    "redacted" : [
        "password"
    ]
}
```

>NOTE: The password value is not returned, but the `true` value indicates that a non empty password has been set.

#### Response

| Code | Meaning                             |
| ---- | -------                             |
| 200  | Success                             |
| 403  | Config override service not enabled |

### Overriding the configuration

To override a value in the configuration make a POST request to the desired path.
The request should contain a JSON object describing what should be modified.

Use the following top level actions:

| Key    | Description                                                          |
| ---    | -----------                                                          |
| set    | Set the value in the configuration overrides.                        |
| delete | Delete the value from the configuration overrides.                   |
| add    | Add a new element to a list configuration section.                   |
| remove | Remove a previously added element from a list configuration section. |

Configuration options not specified in the request will be left unmodified.

#### Example

To disable the SMTP alert handler:

```
POST /kapacitor/v1/config/smtp/
{
    "set":{
        "enabled": false
    }
}
```

To delete the override for the SMTP alert handler:

```
POST /kapacitor/v1/config/smtp/
{
    "delete":[
        "enabled"
    ]
}
```

Actions can be combined in a single request.
Enable the SMTP handler, set its host and remove the port override.

```
POST /kapacitor/v1/config/smtp/
{
    "set":{
        "enabled": true,
        "host": "smtp.example.com"
    },
    "delete":[
        "port"
    ]
}
```

Add a new InfluxDB cluster:

```
POST /kapacitor/v1/config/influxdb
{
    "add":{
        "name": "example",
        "urls": ["https://influxdb.example.com:8086"],
        "default": true,
        "disable-subscriptions": true
    }
}
```

Remove an existing InfluxDB cluster override:

```
POST /kapacitor/v1/config/influxdb
{
    "remove":[
        "example"
    ]
}
```

>NOTE: Only the overrides can be removed, this means that InfluxDB clusters that exist in the configuration cannot be removed.

Modify an existing InfluxDB cluster:

```
POST /kapacitor/v1/config/influxdb/remote
{
    "set":{
        "disable-subscriptions": false,
    },
    "delete": [
        "default"
    ]
}
```

#### Response

| Code | Meaning                                                   |
| ---- | -------                                                   |
| 200  | Success                                                   |
| 403  | Config override service not enabled                       |
| 404  | The specified configuration section/option does not exist |

## Testing Services

Kapacitor makes use of various service integrations.
The following API endpoints provide way for a user to run simple tests to ensure that a service is configured correctly.

### Listing testable services

A list of services that can be tested is available at the `/kapacitor/v1/service-tests` endpoint

| Query Parameter | Default | Purpose                                                                                                                                                               |
| --------------- | ------- | -------                                                                                                                                                               |
| pattern         | *       | Filter results based on the pattern. Uses standard shell glob matching on the service name, see [this](https://golang.org/pkg/path/filepath/#Match) for more details. |


#### Example

```
GET /kapacitor/v1/service-tests
```

```
{
    "link": {"rel":"self", "href": "/kapacitor/v1/service-tests"},
    "services" : [
        {
            "link": {"rel":"self", "href": "/kapacitor/v1/service-tests/influxdb"},
            "name": "influxdb",
            "options": {
                "cluster": ""
            }
        },
        {
            "link": {"rel":"self", "href": "/kapacitor/v1/service-tests/slack"},
            "name": "slack",
            "options": {
                "message": "test slack message",
                "channel": "#alerts",
                "level": "CRITICAL"
            }
        },
        {
            "link": {"rel":"self", "href": "/kapacitor/v1/service-tests/smtp"},
            "name": "smtp",
            "options": {
                "to": ["user@example.com"],
                "subject": "test subject",
                "body": "test body"
            }
        }
    ]
}
```

#### Response

| Code | Meaning |
| ---- | ------- |
| 200  | Success |


### Testing a service

To test a service make a POST request to the `/kapacitor/v1/service-tests/<service name>` endpoint.
The contents of the POST body depend on the service in test.
To determine the available options use a GET request to the same endpoint.
The returned options are also the defaults.

#### Example

See available/default options for the slack service:

```
GET /kapacitor/v1/service-tests/slack
```

```json
{
    "link": {"rel":"self", "href": "/kapacitor/v1/service-tests/slack"},
    "name": "slack"
    "options": {
        "message": "test slack message",
        "channel": "#alerts",
        "level": "CRITICAL"
    }
}
```

Test the slack service integration using custom options:

```
POST /kapacitor/v1/service-tests/slack
{
    "message": "my custom test message",
    "channel": "@user",
    "level": "OK"
}
```
A successful response looks like:

```json
{
    "success": true,
    "message": ""
}
```

A failed response looks like:

```json
{
    "success": false,
    "message": "could not connect to slack"
}
```


#### Response

| Code | Meaning                                                                                         |
| ---- | -------                                                                                         |
| 200  | Success, even if the service under test fails a 200 is returned as the test complete correctly. |


## Miscellaneous

### Ping

You can 'ping' the Kapacitor server to validate you have a successful connection.
A ping request does nothing but respond with a 204.

>NOTE: The Kapacitor server version is returned in the `X-Kapacitor-Version` HTTP header on all requests.
Ping is a useful request if you simply need the verify the version of server you are talking to.

#### Example

```
GET /kapacitor/v1/ping
```

#### Response

| Code | Meaning |
| ---- | ------- |
| 204  | Success |


### Debug Vars

Kapacitor also exposes several statistics and information about its runtime.
These can be accessed at the `/kapacitor/v1/debug/vars` endpoint.

#### Example

```
GET /kapacitor/v1/debug/vars
```

### Debug Pprof

Kapacitor also the standard Go [net/http/pprof](https://golang.org/pkg/net/http/pprof/) endpoints.

```
GET /kapacitor/v1/debug/pprof/...
```

>NOTE: Not all of these endpoints return JSON content.

### Routes

Displays available routes for the API

```
GET /kapacitor/v1/:routes
```
