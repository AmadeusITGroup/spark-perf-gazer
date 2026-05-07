# Databricks Setup

## Configuring `spark.extraListeners`

When you configure PerfGazer via `spark.extraListeners` (see [here](setup_spark_properties.md)), you override the default Databricks listener that powers the post-mortem Spark UI. To keep the Spark UI working, include the Databricks event logging listener alongside PerfGazer, separated by a comma:

```
spark.extraListeners=com.amadeus.perfgazer.PerfGazer,com.databricks.backend.daemon.driver.DBCEventLoggingListener
```

> If you do **not** set `spark.extraListeners` yourself, Databricks registers its listener automatically and you don't need to worry about this.

## Installing the JAR

If you configure PerfGazer via `spark.extraListeners` (i.e. not bundled in your application), the JAR must be on the Databricks classpath before Spark initializes. Use an init script for this:

1. Upload the spark-perf-gazer JAR to DBFS (or another location accessible by the cluster, like a Volume).
2. Create an [init script](https://docs.databricks.com/en/init-scripts/index.html) that copies it at startup. For example:
    ```shell
    cp -f /dbfs/<some_path>/perfgazer_spark_<some_version>.jar /databricks/jars
    ```
3. Attach the init script to your cluster or job configuration.

> If you use PerfGazer [via code](setup_code.md) instead, you can include it as a dependency in your fat JAR and skip the init script entirely.

## Output Destination

As of the latest release, PerfGazer uses the POSIX file interface on the driver to write its output. On Databricks this means you need a POSIX-compatible path, which currently limits you to a **DBFS mount point**:

```
--conf spark.perfgazer.sink.json.destination=/dbfs/mnt/<your_mount>/<path>/applicationId={{spark.app.id}}/
```

> Support for writing directly to cloud storage without a mount point is planned for a future release.

