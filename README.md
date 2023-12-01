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
- investigate predicate pushdowns
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

The core class is `SparklEar`.
It can be registered as `Spark` listener via `spark.sparkContext.addSparkListener(spe)`

It will from then on listen to multiple events coming from `Spark`.

The method `def reports: List[Report]` will return a list of reports representing either an: 

- SQL query (a `SqlReport`)
- job (a `JobReport`)
- stage (a `StageReport`)

The instance of `SparklEar` listens to `Spark` events, and collects the status objects 
in the form of `Wrappers` (objects coming from Spark like `StageInfo`, `SparkListenerJobEnd`, Metrics, ...).

When the reports are requested, all collected `Wrapper`s are inspected and transformed into `Report`s according
to the type of `Wrapper`. A `Report` knows how to get serialized so that a `StringReport` is generated from it.

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

// TODO make clear with the type names at which level we are
// SqlWrapper => put in Map, allows to build SqlSummary
// JobWrapper => put in Map, allows to build JobSummary
// SqlSummary => thing that becomes json
// JobSummary => thing that becomes json
// SparkStuffInitial => Wrapper in map => end + Metrics resolution + Wrapper = Summary

// https://docs.databricks.com/en/clusters/configure.html#cluster-log-delivery
//For example, if the log path is dbfs:/cluster-logs, the log files for a specific cluster will be stored in dbfs:/cluster-logs/<cluster-name> and the individual event logs will be stored in dbfs:/cluster-logs/<cluster-name>/eventlog/<cluster-name-cluster-ip>/<log-id>/.
// spark.eventLog.enabled true
// spark.eventLog.dir dbfs:/databricks/unravel/eventLogs/
// spark.eventLog.enabled true
// spark.eventLog.dir hdfs://namenode/shared/spark-logs
