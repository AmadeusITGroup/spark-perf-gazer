# Setup via Spark Properties

This approach requires no code changes. You only need the PerfGazer JAR on the classpath.

A typical usage via `spark-shell` is shown below (for `spark-submit` it is similar).
Use the latest release version: ![GitHub Release](https://img.shields.io/github/v/release/AmadeusITGroup/spark-perf-gazer).

```
spark-shell \
  --packages io.github.amadeusitgroup:perfgazer_spark_3-5-2_2.12:0.0.1 \
  --conf spark.driver.bindAddress=127.0.0.1 \
  --conf spark.driver.host=127.0.0.1 \
  --conf spark.extraListeners=com.amadeus.perfgazer.PerfGazer \
  --conf spark.perfgazer.sink.class=com.amadeus.perfgazer.JsonSink \
  --conf spark.perfgazer.sink.json.destination=/tmp/perfgazer/jsonsink/date={{perfgazer.now.year}}-{{perfgazer.now.month}}-{{perfgazer.now.day}}/applicationId={{spark.app.id}}
```

> Note: `spark.driver.bindAddress` and `spark.driver.host` force Spark to bind to the loopback interface (`127.0.0.1`). This is required on macOS to prevent the OS firewall from blocking Spark's internal Netty RPC channel. Without these settings, macOS may prompt to allow network access and fail if denied.

## Available properties

### PerfGazer settings

| Property | Default | Description |
|---|---|---|
| `spark.perfgazer.sql.enabled` | `true` | Enable/disable SQL-level metrics collection |
| `spark.perfgazer.jobs.enabled` | `true` | Enable/disable job-level metrics collection |
| `spark.perfgazer.stages.enabled` | `true` | Enable/disable stage-level metrics collection |
| `spark.perfgazer.tasks.enabled` | `false` | Enable/disable task-level metrics collection |
| `spark.perfgazer.max.cache.size` | `100` | Maximum number of events to keep in memory |
| `spark.perfgazer.sink.class` | — | Fully qualified class name of the sink to use |

### JsonSink settings

| Property | Default | Description |
|---|---|---|
| `spark.perfgazer.sink.json.destination` | — | Destination path for JSON output. Either a local POSIX path (written directly by the driver) or a remote URI (`s3a://`, `abfss://`, `gs://`, `dbfs:/`, `hdfs://`, …), in which case files are first written to a local staging directory on the driver and then promoted to the remote destination (see `spark.perfgazer.sink.json.stagingDir`). Should include a partition that uniquely identifies the run (e.g. `applicationId` or `runId`) so that data from different runs does not get mixed. |
| `spark.perfgazer.sink.json.writeBatchSize` | `100` | Number of records to accumulate before writing to disk |
| `spark.perfgazer.sink.json.fileSizeLimit` | `209715200` (200 MB) | File size threshold before rolling to a new file |
| `spark.perfgazer.sink.json.asyncFlushTimeoutMillisecsKey` | — | Max time between periodic flushes (ms) |
| `spark.perfgazer.sink.json.waitForCloseTimeoutMillisecsKey` | — | Max time to wait for graceful sink close (ms) |
| `spark.perfgazer.sink.json.stagingDir` | `/tmp/perfgazer/{{spark.app.id}}/` | Local staging directory used in HDFS mode (remote `destination` URI). Ignored when `destination` is a local POSIX path. Supports the same placeholders as `destination`. |

### Destination placeholders

The destination path supports `{{key}}` placeholders resolved at runtime:

| Placeholder | Description |
|---|---|
| `{{perfgazer.now.year}}` | Current year (4 digits) |
| `{{perfgazer.now.month}}` | Current month (2 digits) |
| `{{perfgazer.now.day}}` | Current day (2 digits) |
| `{{perfgazer.now.hour}}` | Current hour (2 digits) |
| `{{perfgazer.now.minute}}` | Current minute (2 digits) |
| `{{perfgazer.now.second}}` | Current second (2 digits) |
| `{{perfgazer.runid}}` | JVM-stable UUID, unique per application run |
| `{{spark.*}}` | Any Spark configuration property, e.g. `{{spark.app.id}}` |

Date, time, and runId values are captured once at JVM startup and remain stable across multiple resolutions.

> Note: when writing to a remote `destination`, see [Driver Resources](driver_resources.md) for guidance on staging disk usage, storage locality, and driver CPU.
