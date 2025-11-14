# SPARKLEAR

This repository contains the SparklEar Spark Listener.

SparklEar is a configurable Spark Listener that allows to retrieve important stats about Spark SQL queries, jobs and stages in a post-mortem way.
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

---
## User Guide

### Setup Instructions

#### 1. Install the SparklEar library on interactive clusters (Databricks)

| Runtime Version | Spark Version | Installation Method                |
|-----------------|---------------|------------------------------------|
| 13.3            | Spark 3.4.1   | Upload and install from DBFS       |
| 16.4            | Spark 3.5.2   | Upload and install from Workspace  |

#### 2. Install the SparklEar library on job clusters (Databricks)

> Upload the SparklEar JAR to a location accessible by the job cluster (e.g., DBFS).  
> Use an init script to install the SparklEar library on the job cluster.  
> Example init script to install from DBFS:

```shell
if [ -f "/databricks/jars/sparklear_spark352_2_12_0_0_29_SNAPSHOT.jar" ]; then
  rm -f "/databricks/jars/sparklear_spark352_2_12_0_0_29_SNAPSHOT.jar"
fi

cp -f /dbfs/FileStore/jars/sparklear_spark352_2_12_0_0_29_SNAPSHOT.jar /databricks/jars
```

### Usage Instructions

#### Configuration

> The configuration of the SparklEar listener is done via Spark configuration properties.  
> The following properties are available:
- `spark.sparklear.sql.enabled`: enable/disable sql level metrics collection (default: `true`)
- `spark.sparklear.jobs.enabled`: enable/disable job level metrics collection (default: `true`)
- `spark.sparklear.stages.enabled`: enable/disable stage level metrics collection (default: `true`)
- `spark.sparklear.tasks.enabled`: enable/disable task level metrics collection (default: `false`)
- `spark.sparklear.max.cache.size`: maximum number of events to keep in memory (default: `100` events)
- `spark.sparklear.sink.class`: fully qualified class name of the sink to use (default: `com.amadeus.sparklear.JsonSink`)
- `spark.sparklear.sink.json.destination`: destination path for the JSON sink (if using `JsonSink`)
- `spark.sparklear.sink.json.writeBatchSize`: number of records to reach before writing to disk (if using `JsonSink`, default: `100` records)
- `spark.sparklear.sink.json.fileSizeLimit`: size of JSON file to reach before switching to a new file (if using `JsonSink`, default: `209715200` bytes = `200 MB`)

#### Register the listener programmatically (notebook or application)

##### 1. Register the listener

```scala
import com.amadeus.sparklear.SparklEar
import com.amadeus.sparklear.PathBuilder.PathOps

// Instantiate listener with JSON Sink
val basePath = "/dbfs/sparklear/jsonsink/"
val sparklearConf = new SparkConf()
  .setAll(spark.sparkContext.getConf.getAll)
  .set("spark.sparklear.tasks.enabled", "true")
  .set("spark.sparklear.sink.class", "com.amadeus.sparklear.JsonSink")
  .set("spark.sparklear.sink.json.destination", basePath + "clusterName=${spark.databricks.clusterUsageTags.clusterName}/date=${sparklear.now.year}-${sparklear.now.month}-${sparklear.now.day}/applicationId=${spark.app.id}")
val sparklear = new SparklEar(sparklearConf)

print(sparklear.sink)

// Register listener
spark.sparkContext.addSparkListener(sparklear)
```

> The `JsonSink` uses the POSIX interface on the driver to write JSON files.  
> In order to analyze output directly, it's necessary to configure the listener to write directly to the dbfs mount point.  
> Example: `basePath = "/dbfs/logs/sparklear/jsonsink"` (Databricks)

##### 2. Remove and close the listener

```scala
// Remove listener and flush remaining events to disk by closing it
spark.sparkContext.removeSparkListener(sparklear)
sparklear.close()
print(sparklear.getSnippets)
```

#### Register the listener using cluster configuration (Databricks)

> Go to your cluster configuration page, and add the following Spark configuration properties:
```
spark.extraListeners com.amadeus.sparklear.SparklEar
spark.sparklear.tasks.enabled true
spark.sparklear.sink.class com.amadeus.sparklear.JsonSink
spark.sparklear.sink.json.destination /dbfs/sparklear/jsonsink/clusterName=${spark.databricks.clusterUsageTags.clusterName}/date=${sparklear.now.year}-${sparklear.now.month}-${sparklear.now.day}/applicationId=${spark.app.id}
```
> Customize the destination path as needed.
> The `JsonSink` uses the POSIX interface on the driver to write JSON files.  
> In order to analyze output directly, it's necessary to configure the listener to write directly to the dbfs mount point.

#### Analyze listener data

```sql
CREATE OR REPLACE TEMPORARY VIEW sql
USING json
OPTIONS (
  path "dbfs:/sparklear/jsonsink/clusterName=*/date=*/applicationId=*/sql-reports-*.json",
  basePath "dbfs:/sparklear/jsonsink/"
);
CREATE OR REPLACE TEMPORARY VIEW job
USING json
OPTIONS (
  path "dbfs:/sparklear/jsonsink/clusterName=*/date=*/applicationId=*/job-reports-*.json",
  basePath "dbfs:/sparklear/jsonsink/"
);
CREATE OR REPLACE TEMPORARY VIEW stage
USING json
OPTIONS (
  path "dbfs:/sparklear/jsonsink/clusterName=*/date=*/applicationId=*/stage-reports-*.json",
  basePath "dbfs:/sparklear/jsonsink/"
);
CREATE OR REPLACE TEMPORARY VIEW task
USING json
OPTIONS (
  path "dbfs:/sparklear/jsonsink/clusterName=*/date=*/applicationId=*/task-reports-*.json",
  basePath "dbfs:/sparklear/jsonsink/"
);

SELECT *
  FROM job j
  JOIN stage s ON s.applicationId = j.applicationId AND ARRAY_CONTAINS(j.stages, s.stageId)
  JOIN task t ON t.applicationId = s.applicationId AND t.stageId = s.stageId;
```

```scala
import org.apache.spark.sql.functions._

val sparkPath = basePath.replaceFirst("^/dbfs/", "dbfs:/")
val df_jobs_reports = spark.read.option("basePath",sparkPath).json(sparkPath + "clusterName=*/date=*/applicationId=*/job-reports-*.json")
val df_stages_reports = spark.read.option("basePath",sparkPath).json(sparkPath + "clusterName=*/date=*/applicationId=*/stage-reports-*.json")
val df_tasks_reports = spark.read.option("basePath",sparkPath).json(sparkPath + "clusterName=*/date=*/applicationId=*/task-reports-*.json")

// Reconcile reports from JSON files
val df_tasks = df_jobs_reports
  .withColumn("stageId", explode(col("stages")))
  .drop("stages")
  .join(df_stages_reports, Seq("date", "applicationId", "stageId"))
  .join(df_tasks_reports, Seq("date", "applicationId", "stageId"))

display(df_tasks)
```
> Spark reads files from dbfs directly.  
> Example: `sparkPath = "dbfs:/sparklear/jsonsink/"` (Databricks)
---

## Developers

### Contributor guide

#### Overview

The core class is `SparklEar`, which can be instantiated easily when providing a `Config`.

It can be registered as `Spark` listener via `spark.sparkContext.addSparkListener(...)`.
It will then listen to multiple events coming from `Spark`.

Some event objects at query/job/stage level are stored in memory for later processing.
Those events are wrapped by subtypes of `Event`. They are mostly start events, with some exceptions.
These are preserved in a `CappedConcurrentHashMap` that has a maximum size so that memory usage is limited.
The Spark events wrapped are related to classes like: 

- `org.apache.spark...StageInfo`
- `org.apache.spark...SparkListenerJobEnd`
- ...

When a SQL query, or a job, or a stage finishes, it triggers a callback mechanism. 

When the inputs are requested to `SparklEar`, all collected `Event`s are inspected and transformed into `Entity`s at the end
of the query/job/stage execution enriched with some extra information only available then, according
to the type of `Event`.

A `Entity` is transformed into one (or multiple) `Report`/s.
A `Report` is a type that represents the report unit shared with the end-user.

It is the `Translator` that *translates* a `Entity` into a `Report`.

A `Filter` is a filter that operates on `Report`s, so that the end-user can have some control to focus specific aspects of
their Spark ETL (like *file pruning* for instance).

You can find here a diagram connecting all classes involved in the data transformation from raw (coming from Spark) until
they become a `Report` ready to be exposed to the end-user.

```
TRAITS
-----------------------------------------------------------------
X<:Event -----------> Entity ---------> Report -------> Sink
Y<:Event  
-----------------------------------------------------------------
CLASSES
-----------------------------------------------------------------
SqlEvent ---------> SqlEntity --------> SqlReport
                          
JobEvent -------+-> JobEntity --------> JobReport
StageEvent(s)--/                             
                        
StageEvent -------> StageEntity ------> StageReport
```

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

```bash
# (optional) clean previous local publishes and publish
find ~/.ivy2 -type f -name *sparklear* | xargs rm
sbt publishLocal
# run spark shell with the listener (change the version accordingly)
spark-shell \
--packages com.amadeus:sparklear_spark352_2.12:0.0.29-SNAPSHOT \
--conf spark.extraListeners=com.amadeus.sparklear.SparklEar
```