# Setup via Code Change

This approach configures and registers the listener programmatically. Include the library as a dependency in your project.

This approach is required when the JVM is launched before you can add your properties to it. This seems to be the case in Databricks.

```scala
import com.amadeus.perfgazer.{JsonSink, PerfGazer, PerfGazerConfig}

val jsonSink = new JsonSink(
  JsonSink.Config(
    destination = "/dbfs/perfgazer/v1/",
    writeBatchSize = 100,
    fileSizeLimit = 10L * 1024
  ),
  spark.sparkContext.getConf
)

val perfGazerConfig = PerfGazerConfig(
  sqlEnabled = true,
  jobsEnabled = true,
  stagesEnabled = true,
  tasksEnabled = false,
  maxCacheSize = 100
)

val perfGazer = new PerfGazer(perfGazerConfig, jsonSink)

// Register the listener
spark.sparkContext.addSparkListener(perfGazer)

// Your Spark code here ...

// At the end of your application, remove the listener and close it properly
spark.sparkContext.removeSparkListener(perfGazer)
perfGazer.close()
```

> Note: a shutdown hook is registered automatically on construction, so the listener will be closed on JVM termination even if you omit the explicit `removeSparkListener`/`close()` calls. That said, calling them explicitly at the end of your application is still good practice to ensure a clean, predictable teardown.


> Note: the destination should include a partition that uniquely identifies the application run (e.g. `applicationId={{spark.app.id}}` or `runId={{perfgazer.runid}}`) so that data from different runs does not get mixed. See [destination placeholders](setup_spark_properties.md#destination-placeholders) for available placeholders.


> Note: when `destination` is a remote URI (`s3a://`, `abfss://`, `gs://`, `dbfs:/`, `hdfs://`, …), the sink first writes files to a local staging directory and then promotes them to the remote destination. The staging directory defaults to `/tmp/perfgazer/{{spark.app.id}}/` and can be overridden via the `stagingDir` field of `JsonSink.Config` (it supports the same [placeholders](setup_spark_properties.md#destination-placeholders) as `destination`):
>
> ```scala
> JsonSink.Config(
>   destination = "abfss://container@account.dfs.core.windows.net/perfgazer/v1/applicationId={{spark.app.id}}/",
>   stagingDir = "/local/ssd/perfgazer/{{spark.app.id}}/"
> )
> ```


> Note: when writing to a remote `destination`, see [Driver Resource Considerations](driver_resources.md) for guidance on staging disk usage, storage locality, and driver CPU.
