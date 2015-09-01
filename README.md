# Kapacitor
Open source framework for processing, monitoring, and alerting on time series data

# Proposed workflow

1. Start Kapacitor

    ```sh
    $ kapacitord
    ```

2. Start a data stream.
  * Start a telegraf process or,
  * Configure InfluxDB to forward on data to Kapacitor
3. Put Kapacitor into record mode

    ```sh
    $ kapacitor start-recording
    ```

4. Wait for a bit.
5. Stop recording

    ```sh
    $ kapacitor stop-recording
    RecordingID=28437519
    ```

6. Define kapacitor plugin

    ```sh
    $ kapacitor define --where ... --script path/to/dsl/script --name my_alert_script
    ```

7. Test plugin. Replay recording to just your plugin

    ```sh
    $ kapacitor replay 28437519 my_alert_script
    ```

8. Edit kapacitor plugin and test until its working

    ```sh
    $ kapacitor define --where ... --script path/to/dsl/script --name my_alert_script
    $ kapacitor replay 28437519 my_alert_script
    ```

9. Enable the plugin once you are satisfied that it is working

    ```sh
    $ kapacitor enable my_alert_script
    ```

# Components

Below are the logical components to make the workflow  possible.

* Kapacitor daemon `kapacitord` that listens on a net/unix socket and manages the rest of the components.
* Matching -- uses the `where` clause of a plugin to map points in the data stream to a plugin instance
* Interpreter for DSL -- executes the DSL based on incoming metrics
* Replay engine -- records and replays bits of the data stream. Can replay metrics to independent streams so testing can be done in isolation of live stream.
* Plugin manager -- handles defining, updating and shipping plugins around.
* API -- HTTP API for accessing method of other components
* CLI -- `kapacitor` command line utility to call the HTTP API


# Questions

*  Q: How do we scale beyond a single instance?
  * A: We could use the matching component to shard and route different series within the stream to the specific nodes within the cluster. AKA consistent sharding.
  * A: We could make the DSL and plugins in general a Map-Reduce framework so that each instance only handles a portion of the stream
  * Other ideas?

* Q: How should we treat scheduled plugins vs stream plugins?
  * A: We could treat them the same but just ones that have `where` clauses are stream plugins and ones that have a schedule and a `select` statement are scheduled plugins.
  * A: We could treat them as completely different entities and the workflow would be different for each. Seems like they naturally have different workflows so maybe this is better.

