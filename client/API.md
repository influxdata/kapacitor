# Kapacitor API Reference Documentation

Kapacitor provides an HTTP API on port 9092 by default.
With the API you can control which tasks are executing, query status of tasks and manage recordings etc.

Each section below defines the available API endpoints and there inputs and outputs.

All requests are versioned and namespaced using the root path `/kapacitor/v1/`.

### Response Codes

All requests can return these response codes:

| HTTP Response Code | Meaning                                                                                                                                                             |
| ------------------ | -------                                                                                                                                                             |
| 2xx                | The request was a success the content is dependent on the request.                                                                                                  |
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

## Writing Data

Kapacitor can accept writes over HTTP using the line protocol.
This endpoint is identical in nature to the InfluxDB write endpoint.

| Query Parameter | Purpose                               |
| --------------- | -------                               |
| db              | Database name for the writes.         |
| rp              | Retention policy name for the writes. |

>NOTE: Kapacitor scopes all points by their database and retention policy. This means you MUST specify the `rp` for writes or Kapacitor will not know which retention policy to use.

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

To define a task POST to the `/kapacitor/v1/tasks/` endpoint.
If a task already exists then use the `PATCH` method to modify any property of the task.

Define a task using a JSON object with the following options:

| Property | Purpose                                                                |
| -------- | -------                                                                |
| id       | Unique identifier for the task. If empty a random ID will be chosen. |
| type     | The task type: `stream` or `batch`.                                    |
| dbrps    | List of database retention policy pairs the task is allowed to access. |
| script   | The content of the script.                                             |
| enabled  | Whether the task is enabled or disabled.                               |

When using PATCH, if any option is missing it will be left unmodified.

#### Example

Create a new task with ID TASK_ID.

```
POST /kapacitor/v1/tasks/
{
    "id" : "TASK_ID",
    "type" : "stream",
    "dbrps": [{"db": "DATABASE_NAME", "rp" : "RP_NAME"}],
    "script": "stream\n    |from()\n        .measurement('cpu')\n"
}
```

Response with task id and link.

```
{
    "id" : "TASK_ID",
    "link" : {"rel": "self", "href": "/kapacitor/v1/tasks/TASK_ID"}
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


Enable an existing task.

```
PATCH /kapacitor/v1/tasks/TASK_ID
{
    "enabled" : true,
}
```

Disable an existing task.

```
PATCH /kapacitor/v1/tasks/TASK_ID
{
    "enabled" : false,
}
```

Define a new task that is enabled on creation.

```
POST /kapacitor/v1/tasks/TASK_ID
{
    "type" : "stream",
    "dbrps": [{"db": "DATABASE_NAME", "rp" : "RP_NAME"}],
    "script": "stream\n    |from()\n        .measurement('cpu')\n"
    "enabled" true,
}
```

Response with task id and link.

```
{
    "id" : "TASK_ID",
    "link" : {"rel": "self", "href": "/kapacitor/v1/tasks/TASK_ID"}
}
```

#### Response

| Code | Meaning                                     |
| ---- | -------                                     |
| 200  | Success, contains id and link for new task. |
| 204  | Success                                     |
| 404  | Task does not exist                         |

### Get Task

To get information about a task make a GET request to the `/kapacitor/v1/tasks/TASK_ID` endpoint.

| Query Parameter | Default    | Purpose                                                                                                                          |
| --------------- | -------    | -------                                                                                                                          |
| dot-view        | attributes | One of `labels` or `attributes`. Labels is less readable but will correctly render with all the information contained in labels. |
| script-format   | formatted  | One of `formatted` or `raw`. Raw will return the script identical to how it was defined. Formatted will first format the script. |


A task has these read only properties in addition to the properties listed [above](#define-task).

| Property  | Description                                                                                                                     |
| --------  | -----------                                                                                                                     |
| dot       | [GraphViz DOT](https://en.wikipedia.org/wiki/DOT_(graph_description_language)) syntax formatted representation of the task DAG. |
| executing | Whether the task is currently executing.                                                                                        |
| error     | Any error encountered when executing the task.                                                                                  |
| stats     | Map of statistics about a task.                                                                                                 |

#### Example

Get information about a task using defaults.

```
GET /kapacitor/v1/tasks/TASK_ID
```

```
{
    "link" : {"rel": "self", "href": "/kapacitor/v1/tasks/TASK_ID"},
    "id" : "TASK_ID",
    "type" : "stream",
    "dbrps": [{"db": "DATABASE_NAME", "rp" : "RP_NAME"}],
    "script": "stream\n    |from()\n        .measurement('cpu')\n",
    "dot" : "digraph TASK_ID { ... }",
    "enabled" : true,
    "executing" : true,
    "error" : "",
    "stats": {}
}
```

Get information about a task using only labels in the DOT content and skip the format step.

```
GET /kapacitor/v1/tasks/TASK_ID?dot-view=labels&script-format=raw
```

```
{
    "link" : {"rel": "self", "href": "/kapacitor/v1/tasks/TASK_ID"},
    "id" : "TASK_ID",
    "type" : "stream",
    "dbrps": [{"db": "DATABASE_NAME", "rp" : "RP_NAME"}],
    "script": "stream|from().measurement('cpu')",
    "dot" : "digraph TASK_ID { ... }",
    "enabled" : true,
    "executing" : true,
    "error" : "",
    "stats": {}
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

| Code | Meaning             |
| ---- | -------             |
| 204  | Success             |

>NOTE: Deleting a non-existent task is not an error and will return a 204 success.


### List Tasks

To get information about several tasks make a GET request to the `/kapacitor/v1/tasks` endpoint.

| Query Parameter | Default    | Purpose                                                                                                                                           |
| --------------- | -------    | -------                                                                                                                                           |
| pattern         |            | Filter results based on the pattern. Uses standard shell glob matching, see [this](https://golang.org/pkg/path/filepath/#Match) for more details. |
| fields          |            | List of fields to return. If empty returns all fields. Fields `id` and `link` are always returned.                                              |
| dot-view        | attributes | One of `labels` or `attributes`. Labels is less readable but will correctly render with all the information contained in labels.                  |
| script-format   | formatted  | One of `formatted` or `raw`. Raw will return the script identical to how it was defined. Formatted will first format the script.                  |
| offset          | 0          | Offset count for paginating through tasks.                                                                                                        |
| limit           | 100        | Maximum number of tasks to return.                                                                                                                |

#### Example

Get all tasks.

```
GET /kapacitor/v1/tasks
```

```
{
    "tasks" : [
        {
            "link" : "/kapacitor/v1/tasks/TASK_ID",
            "id" : "TASK_ID",
            "type" : "stream",
            "dbrps": [{"db": "DATABASE_NAME", "rp" : "RP_NAME"}],
            "script": "stream|from().measurement('cpu')",
            "dot" : "digraph TASK_ID { ... }",
            "enabled" : true,
            "executing" : true,
            "error" : "",
            "stats": {}
        },
        {
            "link" : "/kapacitor/v1/tasks/ANOTHER_TASK_ID",
            "id" : "ANOTHER_TASK_ID",
            "type" : "stream",
            "dbrps": [{"db": "DATABASE_NAME", "rp" : "RP_NAME"}],
            "script": "stream|from().measurement('cpu')",
            "dot" : "digraph ANOTHER_TASK_ID{ ... }",
            "enabled" : true,
            "executing" : true,
            "error" : "",
            "stats": {}
        }
    ]
}
```

Optionally specify a glob `pattern` to list only matching tasks.

```
GET /kapacitor/v1/task?pattern=TASK*
```

```
{
    "tasks" : [
        {
            "link" : "/kapacitor/v1/tasks/TASK_ID",
            "id" : "TASK_ID",
            "type" : "stream",
            "dbrps": [{"db": "DATABASE_NAME", "rp" : "RP_NAME"}],
            "script": "stream|from().measurement('cpu')",
            "dot" : "digraph TASK_ID { ... }",
            "enabled" : true,
            "executing" : true,
            "error" : "",
            "stats": {}
        }
    ]
}
```

Get all tasks, but only the enabled, executing and error fields.

```
GET /kapacitor/v1/tasks?fields=enabled&fields=executing&fields=error
```

```
{
    "tasks" : [
        {
            "link" : "/kapacitor/v1/tasks/TASK_ID",
            "id" : "TASK_ID",
            "enabled" : true,
            "executing" : true,
            "error" : "",
        },
        {
            "link" : "/kapacitor/v1/tasks/ANOTHER_TASK_ID",
            "id" : "ANOTHER_TASK_ID",
            "enabled" : true,
            "executing" : true,
            "error" : "",
        }
    ]
}
```

#### Response

| Code | Meaning                |
| ---- | -------                |
| 200  | Success                |

>NOTE: If the pattern does not match any tasks an empty list will be returned, with a 200 success.

## Replays and Recordings

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
| cluster   | Name of a configured InfluxDB cluster. If empty uses the default cluster.                                        |

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
POST /kapacitor/v1/recording/query
{
    "query" : "SELECT mean(usage_idle) FROM cpu WHERE time > now() - 1h GROUP BY time(10m)",
    "type" : "batch"
}
```

Create a recording with a custom ID.

```
POST /kapacitor/v1/recording/query
{
    "id" : "MY_RECORDING_ID",
    "query" : "SELECT mean(usage_idle) FROM cpu WHERE time > now() - 1h GROUP BY time(10m)",
    "type" : "batch"
}
```

#### Response

All recordings are assigned an ID which is returned in this format with a link.

```
{
    "link" : {"rel": "self", "href": "/kapacitor/v1/recordings/e24db07d-1646-4bb3-a445-828f5049bea0"},
    "id" : "e24db07d-1646-4bb3-a445-828f5049bea0"
}
```

| Code | Meaning                             |
| ---- | -------                             |
| 201  | Success, the recording has started. |

### Wait for Recording

In order to determine when a recording has finished you must make a GET request to the returned link typically something like `/kapacitor/v1/recordings/RECORDING_ID`.

A recording has these read only properties.

| Property | Description                                                                               |
| -------- | -----------                                                                               |
| id       | A unique identifier for the recording.                                                    |
| type     | One of `stream` or `batch`. A recording cannot be replayed to a task of a different type. |
| size     | Size of the recording on disk in bytes.                                                   |
| date     | Date the recording finished.                                                              |
| error    | Any error encountered when creating the recording.                                        |
| status   | One of `running` or `finished`.                                                           |
| progress | Number between 0 and 1 indicating the approximate progress of the recording.              |


#### Example

```
GET /kapacitor/v1/recordings/e24db07d-1646-4bb3-a445-828f5049bea0
```

```
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

```
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

#### Response

| Code | Meaning                                            |
| ---- | -------                                            |
| 200  | Success, the recording is finished.                |
| 202  | Success, the recording exists but is not finished. |
| 404  | No such recording exists.                          |

### Delete Recording

To delete a recording make a DELETE request to the `/kapacitor/v1/recordings/RECORDING_ID` endpoint.

```
DELETE /kapacitor/v1/recordings/RECORDING_ID
```

#### Response

| Code | Meaning             |
| ---- | -------             |
| 204  | Success             |

>NOTE: Deleting a non-existent recording is not an error and will return a 204 success.

### List Recordings

To list all recordings make a GET request to the `/kapacitor/v1/recordings` endpoint.
Recordings are sorted by date.

| Query Parameter | Default | Purpose                                    |
| --------------- | ------- | -------                                    |
| offset          | 0       | Offset count for paginating through tasks. |
| limit           | 100     | Maximum number of tasks to return.         |

#### Example

```
GET /kapacitor/v1/recordings
```

```
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


### Replay Recording

To replay a recording make a POST request to `/kapacitor/v1/replays/`

| Parameter      | Default | Purpose                                                                                                                                                                                                                                          |
| ----------     | ------- | -------                                                                                                                                                                                                                                          |
| id             |         | Unique identifier for the replay. If empty a random ID is chosen.                                                                                                                                                                                |
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

```
{
    "link" : {"rel": "self", "href": "/kapacitor/v1/replays/ad95677b-096b-40c8-82a8-912706f41d4c"},
    "id" : "ad95677b-096b-40c8-82a8-912706f41d4c",
}
```

| Code | Meaning                                         |
| ---- | -------                                         |
| 201  | Success, replay has started.                    |
| 404  | The specified task or recording does not exist. |

### Waiting for a Replay

Like recordings you make a GET request to the `/kapacitor/v1/replays/REPLAY_ID` endpoint to get the status of the replay.

A replay has these read only properties in addition to the properties listed [above](#replay-recording).

| Property | Description                                                               |
| -------- | -----------                                                               |
| status   | One of `running` or `finished`.                                           |
| progress | Number between 0 and 1 indicating the approximate progress of the replay. |


#### Example

Get the status of a replay.

```
GET /kapacitor/v1/replays/ad95677b-096b-40c8-82a8-912706f41d4c
```

```
{
    "link" : {"rel": "self", "href": "/kapacitor/v1/replays/ad95677b-096b-40c8-82a8-912706f41d4c"},
    "id" : "ad95677b-096b-40c8-82a8-912706f41d4c",
    "task" : "TASK_ID",
    "recording" : "RECORDING_ID",
    "clock" : "fast",
    "recording-time" : false,
    "status" : "running",
    "progress" : 0.57
}
```

Once the replay is complete.

```
GET /kapacitor/v1/replays/ad95677b-096b-40c8-82a8-912706f41d4c
```

```
{
    "link" : {"rel": "self", "href": "/kapacitor/v1/replays/ad95677b-096b-40c8-82a8-912706f41d4c"},
    "id" : "ad95677b-096b-40c8-82a8-912706f41d4c",
    "task" : "TASK_ID",
    "recording" : "RECORDING_ID",
    "clock" : "fast",
    "recording-time" : false,
    "status" : "finished",
    "progress" : 1
}
```

### List Replays

You can list replays for a given recording by making a GET request to `/kapacitor/v1/replays`.

| Query Parameter | Default | Purpose                                    |
| --------------- | ------- | -------                                    |
| offset          | 0       | Offset count for paginating through tasks. |
| limit           | 100     | Maximum number of tasks to return.         |

#### Example

```
GET /kapacitor/v1/replays
```

```
{
    "replays" [
        {
            "link" : {"rel": "self", "href": "/kapacitor/v1/replays/ad95677b-096b-40c8-82a8-912706f41d4c"},
            "id" : "ad95677b-096b-40c8-82a8-912706f41d4c",
            "task" : "TASK_ID",
            "recording" : "RECORDING_ID",
            "clock" : "fast",
            "recording-time" : false,
            "status" : "finished",
            "progress" : 1
        },
        {
            "link" : {"rel": "self", "href": "/kapacitor/v1/replays/be33f0a1-0272-4019-8662-c730706dac7d"},
            "id" : "be33f0a1-0272-4019-8662-c730706dac7d",
            "task" : "TASK_ID",
            "recording" : "RECORDING_ID",
            "clock" : "fast",
            "recording-time" : false,
            "status" : "finished",
            "progress" : 1
        }
    ]
}
```


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
