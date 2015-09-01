# Kapacitor
Open source framework for processing, monitoring, and alerting on time series data

# Proposed workflow

1. Start Kapacitor

    ```sh
    $ kapacitord
    ```

2. Start a data stream. Configure telegraf with an output to Kapacitor.
3. Create a data snapshot
  * Select data from an existing InfluxDB host and save it:

      ```sh
      $ kapacitor record --query 'select value from cpu_idle where time > start and time < stop'
      RecordingID=2869246
      ```
  * Or record the live stream for a bit:

      ```sh
      $ kapacitor start-recording
      $ sleep 60
      $ kapacitor stop-recording
      RecordingID=2869246
      ```

4. Define kapacitor plugin

    ```sh
    $ kapacitor define --where "key = 'value' and key2 = 'other_value'" --script path/to/dsl/script --name my_alert_script
    ```

5. Test plugin. Replay recording to just your plugin

    ```sh
    $ kapacitor replay 2869246 my_alert_script
    ```

6. Edit kapacitor plugin and test until its working

    ```sh
    $ kapacitor define --where ... --script path/to/dsl/script --name my_alert_script
    $ kapacitor replay 2869246 my_alert_script
    ```

7. Enable or push the plugin once you are satisfied that it is working

    ```sh
    $ # enable the plugin locally
    $ kapacitor enable my_alert_script
    $ # or push the tested plugin to a prod server
    $ kapacitor push --remote address_to_remote_kapacitor my_alert_script
    ```

# Components

Below are the logical components to make the workflow  possible.

* Kapacitor daemon `kapacitord` that listens on a net/unix socket and manages the rest of the components.
* Matching -- uses the `where` clause of a plugin to map points in the data stream to a plugin instance.
* Interpreter for DSL -- executes the DSL based on incoming metrics.
* Replay engine -- records and replays bits of the data stream. Can replay metrics to independent streams so testing can be done in isolation of live stream. Can also save the result of a query for replaying.
* Plugin manager -- handles defining, updating and shipping plugins around.
* API -- HTTP API for accessing method of other components.
* CLI -- `kapacitor` command line utility to call the HTTP API.


# Questions

*  Q: How do we scale beyond a single instance?
  * A: We could use the matching component to shard and route different series within the stream to the specific nodes within the cluster. AKA consistent sharding.
  * A: We could make the DSL and plugins in general a Map-Reduce framework so that each instance only handles a portion of the stream.
      We will need to provide an all-in-one solution so that you don't have to deal with the complexities MR if you don't want to.
  * Other ideas?

* Q: How should we treat scheduled plugins vs stream plugins?
  * A: We could treat them as completely different entities and the workflow would be different for each. Seems like they naturally have different workflows so maybe this is better.
  * A: We could treat them the same but just ones that have `where` clauses are stream plugins and ones that have a schedule and a `select` statement are scheduled plugins. Not a good idea right now.

