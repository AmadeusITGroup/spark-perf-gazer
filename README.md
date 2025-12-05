# PerfGazer, Performance Gazer for Apache Spark

[![License](http://img.shields.io/:license-Apache%202-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)
[![Build](https://github.com/AmadeusITGroup/spark-perf-gazer/actions/workflows/build.yml/badge.svg)](https://github.com/AmadeusITGroup/spark-perf-gazer/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/AmadeusITGroup/spark-perf-gazer/graph/badge.svg?token=ZC4fzrZxI7)](https://codecov.io/gh/AmadeusITGroup/spark-perf-gazer)

This repository contains the PerfGazer Spark Listener.

PerfGazer is a configurable Spark Listener that allows to retrieve important stats about Spark SQL queries, jobs and stages in a post-mortem way.
Users should attach it as a listener to the `SparkSession`. It will cause to report certain lines in the logs, which can be interpreted afterwards.

## Use cases

The use-cases that this library is intended to address:

- allow to do post-mortem analysis of Spark SQL queries, jobs and stages programmatically
- measure Spark jobs / stages accumulated in-executor durations
- identify jobs that take the longer cumulated execution time (as measured in executors)
- identify Spark jobs that have spill
- monitor certain SQL metrics like amount of files read, pruned, ...
- investigate predicate pushdowns and their effectiveness on data skipping
- connect to any monitoring system to expose certain metrics (spill, files read, ...)
- ...

## Why not just the Spark UI?

There are some problems with the analysis of execution stats from the Spark UI:
- the process is mostly manual (UI navigation)
- it is often slow (takes time to load the UI)
- has a limited sql queries / jobs retention (so stats data is often purged for large applications)
- not made for analytics (i.e. how many of my SQL queries used 'BroadcastHashJoin'?)

---
## User Guide

### Setup Instructions

To use PerfGazer, you need to:
- either put the library JAR in the classpath of your Spark application, if you configure the listener via Spark properties (see [Configuration via Spark properties](#configuration-via-spark-properties))
- or include the library as a dependency in your project, if you configure the listener programmatically (see [Configuration via code change](#configuration-via-code-change))

For Databricks setup instructions, see [this](docs/databricks.md).

### Usage Instructions

#### Listener configuration

The configuration and registration of the PerfGazer listener can be done either via Spark configuration properties
or programmatically via code change.

##### Configuration via Spark properties

A typical usage of PerfGazer via `spark-shell` is the following (for `spark-submit` it is similar):

```
spark-shell \
  --packages com.amadeus:perfgazer_spark_3-5-2_2.12:0.0.1 \
  --conf spark.extraListeners=com.amadeus.perfgazer.PerfGazer \
  --conf spark.perfgazer.sink.class=com.amadeus.perfgazer.JsonSink \
  --conf spark.perfgazer.sink.json.destination=/tmp/perfgazer/jsonsink/date={{perfgazer.now.year}}-{{perfgazer.now.month}}-{{perfgazer.now.day}}/applicationId={{spark.app.id}}
```
Change the versions as needed.

The following properties are available to configure the PerfGazer listener and its sink:

PerfGazer settings:
- `spark.perfgazer.sql.enabled`: enable/disable sql level metrics collection (default: `true`)
- `spark.perfgazer.jobs.enabled`: enable/disable job level metrics collection (default: `true`)
- `spark.perfgazer.stages.enabled`: enable/disable stage level metrics collection (default: `true`)
- `spark.perfgazer.tasks.enabled`: enable/disable task level metrics collection (default: `false`)
- `spark.perfgazer.max.cache.size`: maximum number of events to keep in memory (default: `100` events)
- `spark.perfgazer.sink.class`: fully qualified class name of the sink to use

Json Sink settings:
- `spark.perfgazer.sink.json.destination`: destination path for the JSON sink (if using `JsonSink`)
- `spark.perfgazer.sink.json.writeBatchSize`: number of records to reach before writing to disk (if using `JsonSink`, default: `100` records)
- `spark.perfgazer.sink.json.fileSizeLimit`: size of JSON file to reach before switching to a new file (if using `JsonSink`, default: `209715200` bytes = `200 MB`)
- `spark.perfgazer.sink.json.asyncFlushTimeoutMillisecsKey`: maximum time to wait regularly before flushing reports to disk (in milliseconds)
- `spark.perfgazer.sink.json.waitForCloseTimeoutMillisecsKey`: maximum time to wait for graceful close of the sink (in milliseconds)

Note: the `JsonSink` uses the POSIX interface on the driver to write JSON files.

PerfGazer comes with a shutdown hook that ensures that the listener is closed gracefully on driver JVM shutdown.

##### Configuration via code change

You can configure and register the listener in your Scala code as follows:

```scala
import com.amadeus.perfgazer.{JsonSink, PerfGazer, PerfGazerConfig}

val jsonSink = new JsonSink(
  JsonSink.Config(
    destination = "/dbfs/perfgazer/v1/",
    writeBatchSize = 100,
    fileSizeLimit = 10L*1024
  ),
  spark.conf
)

val perfGazerConfig = PerfGazerConfig(
  sqlEnabled = true,
  jobsEnabled = true,
  stagesEnabled = true,
  tasksEnabled = false,
  maxCacheSize = 100
)

val perfGazer = new PerfGazer(perfGazerConfig, jsonSink)

// register the listener
spark.sparkContext.addSparkListener(perfgazer)

// your spark code here ...

// At the end of your application ensure you remove the listener and close it properly
spark.sparkContext.removeSparkListener(perfgazer)
perfgazer.close()
```

#### Analyze listener data

##### 1. Analyze using SQL

To analyze the data produced by PerfGazer, it's helpful to create views for the different types of report (sql, job, stage, tasks).
Note that at application shutdown, the view creation snippets corresponding to your configuration are printed in the logs.

```sql
-- Run the generated snippets to create the view
CREATE OR REPLACE TEMPORARY VIEW sql
...
CREATE OR REPLACE TEMPORARY VIEW job
...
CREATE OR REPLACE TEMPORARY VIEW stage
...
CREATE OR REPLACE TEMPORARY VIEW task
...

-- Query the views
SELECT *
  FROM job j
  JOIN stage s ON s.applicationId = j.applicationId AND ARRAY_CONTAINS(j.stages, s.stageId)
  JOIN task t ON t.applicationId = s.applicationId AND t.stageId = s.stageId;
```

##### 2. Analyze using Scala

```scala
import org.apache.spark.sql.functions._

val sparkPath = "dbfs:/perfgazer/jsonsink/"
val dfJobsReports = spark.read.option("basePath", sparkPath).json(sparkPath + "clusterName=*/date=*/applicationId=*/job-reports-*.json")
val dfStagesReports = spark.read.option("basePath", sparkPath).json(sparkPath + "clusterName=*/date=*/applicationId=*/stage-reports-*.json")
val dfTasksReports = spark.read.option("basePath", sparkPath).json(sparkPath + "clusterName=*/date=*/applicationId=*/task-reports-*.json")

// Reconcile reports from JSON files
val dfTasks = dfJobsReports
  .withColumn("stageId", explode(col("stages")))
  .drop("stages")
  .join(dfStagesReports, Seq("date", "applicationId", "stageId"))
  .join(dfTasksReports, Seq("date", "applicationId", "stageId"))

display(dfTasks)
```
---

## Developers

### Contributor guide

#### Technical overview

Once registered, PerfGazer will listen to multiple events coming from `Spark`.

Some event objects at query/job/stage level are stored in memory for later processing.
Those events are wrapped by subtypes of `Event`. They are mostly start events, with some exceptions.
These are preserved in a `CappedConcurrentHashMap` that has a maximum size so that memory usage is limited.
The Spark events wrapped are related to classes like: 

- `org.apache.spark...StageInfo`
- `org.apache.spark...SparkListenerJobEnd`
- ...

When a SQL query, a job, a stage, or a task finishes, it triggers a callback mechanism. 

When the inputs are requested to `PerfGazer`, all collected `Event`s are inspected and transformed into `Report`s at the end
of the query/job/stage execution enriched with some extra information only available then, according
to the type of `Event`.

A `Report` is a type that represents the report unit shared with the end-user.

A `Filter` is a filter that operates on `Report`s, so that the end-user can have some control to focus specific aspects of
their Spark ETL (like *file pruning* for instance).

#### Build

The project uses `sbt`. 

```sh
sbt test                           # run tests
sbt coverageOn test coverageReport # run tests with coverage checks on
```

#### Dev environment

We use IntelliJ IDEA, you can update the ScalaTest Configuration Template to avoid manual settings.

```
Go to Run -> Edit Configurations -> Edit configuration templates -> ScalaTest 
```

For code formatting setup: 

```
Settings -> Editor -> Code Style -> Scala -> Formatter: ScalaFMT
```

#### Run

You can run a local `spark-shell` with the listener as follows:

```bash
# (optional) clean previous local publishes and publish, for example
find ~/.ivy2 -type f -name *perfgazer* | xargs rm
# publish a local snapshot version
sbt publishLocal
# run spark shell with the listener (change the version accordingly) using the snippet provided above
spark-shell --packages com.amadeus:perfgazer_spark_3.5.2_2.12:0.0.2-SNAPSHOT ...
```

#### Contributing

To contribute to this project, see [this](CONTRIBUTING.md).

## Authors

- Mauricio JOST
- Generoso PAGANO
- Bruno JOUBERT
- Thierry ACCART
- Sergei DOLGOV
- Mathieu TRAMPONT



