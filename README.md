# SPARKLEAR

This repository contains the SparklEar Spark Listener.

SparklEar is a configurable Spark Listener that allows to retrieve important stats about Spark SQL queries, jobs and stages in a post-mortem way.
Users should attach it as a listener to the `SparkSession`. It will cause to report certain lines in the logs, which can be interpreted afterwards.

The use-cases that this library is intended to address:

- allow to do post-mortem analysis of Spark SQL queries, jobs and stages programmatically
- measure Spark jobs / stages accumulated in-executor durations
- identify jobs that take the longer cumulated execution time (as measured in executors)
- identify Spark jobs that have spill
- monitor certain SQL metrics like amount of files read, pruned, ...
- investigate predicate pushdowns and their effectiveness on data skipping
- connect to any monitoring system to expose certain metrics (spill, files read, ...)
- ...

Why not using the regular Spark UI? 
There are some problems with the analysis of execution stats from the Spark UI:
- the process is mostly manual
- it is often slow (takes time to load)
- has a limited retention (so stats data is often loss for large applications)

https://docs.databricks.com/en/_extras/notebooks/source/kb/clusters/event-log-replay.html

## Developers

### Contributor guide

#### Overview

The core class is `SparklEar`, which can be instantiated easily when providing a `Config`.

It can be registered as `Spark` listener via `spark.sparkContext.addSparkListener(...)`.
It will then listen to multiple events coming from `Spark`.

The event objects are collected in the form of `Collect` (classes like `StageInfo`, `SparkListenerJobEnd`, ...).

When the inputs are requested to `SparklEar`, all collected `Collect`s are inspected and transformed into `PreReport`s according
to the type of `Collect`. 

Then, all `PreReport` objects are transformed into multiple `Report` objects. A given `PreReport` can be transformed into 
multiple `Report`s using a `Translator`. A `Report` is a type that represents the report unit shared with the end-user.

#### Sbt

The project uses `sbt`. You can run tests locally with `sbt test`.

#### Intellij
Using IntelliJ IDEA, you can update the ScalaTest Configuration Template to avoid manual settings.

```
Go to Run -> Edit Configurations -> Edit configuration templates -> ScalaTest 
```

For code formatting setup: 

```
Settings -> Editor -> Code Style -> Scala -> Formatter: ScalaFMT
```
# To Do

- Investigate in SparkInternal why V2ExistingTableWriteExec is needed
- Improve README to include comparison with other similar solution
- Add test case with a join
- Investigate if possible to persist SparkUI logs as a complementary approach with
```
-/ https://docs.databricks.com/en/clusters/configure.html#cluster-log-delivery
- For example, if the log path is dbfs:/cluster-logs, the log files for a specific cluster will be stored in dbfs:/cluster-logs/<cluster-name> and the individual event logs will be stored in dbfs:/cluster-logs/<cluster-name>/eventlog/<cluster-name-cluster-ip>/<log-id>/.
-  spark.eventLog.enabled true
-  spark.eventLog.dir dbfs:/databricks/unravel/eventLogs/
-  spark.eventLog.enabled true
-  spark.eventLog.dir hdfs://namenode/shared/spark-logs
```
