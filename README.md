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

## Developers

### Contributor guide

#### Overview

The core class is `SparklEar`, which can be instantiated easily when providing a `Config`.

It can be registered as `Spark` listener via `spark.sparkContext.addSparkListener(...)`.
It will then listen to multiple events coming from `Spark`.

The event objects are collected in the form of `Collect`. These are typically classes like: 

- `org.apache.spark...StageInfo`
- `org.apache.spark...SparkListenerJobEnd`
- ...

When a SQL query, or a job, or a stage finishes, it triggers a callback mechanism. 

When the inputs are requested to `SparklEar`, all collected `Collect`s are inspected and transformed into `PreReport`s according
to the type of `Collect`.

A `PreReport` is transformed into one (or multiple) `Report`/s.
A `Report` is a type that represents the report unit shared with the end-user.

It is the `Translator` that *translates* a `PreReport` into a `Report`.

A `Glass` is a filter that operates on `Report`s, so that the end-user can have some control to focus specific aspects of
their Spark ETL (like *file pruning* for instance).

You can find here a diagram connecting all classes involved in the data transformation from raw (coming from Spark) until
they become a `Report` ready to be exposed to the end-user.

```
TRAITS
------------------------------------------------------------------------------------------------------------------------
<SparkEventDataType>  --> X<:Collect    --->  PreReport -----> (Translator) -----> Report ---> invoke notification(Report)
                          Y<:Collect  
------------------------------------------------------------------------------------------------------------------------

SUBTYPES
------------------------------------------------------------------------------------------------------------------------
 SparkListenerJobEnd      SqlCollect ----+-> SqlPreReport --->   (...)   +-------> StrReport (by SqlTranslator)
 StageInfo                MetricCollect /                                 \------> SqlPlanNodeReport (by SqlTranslator)
 ...
                          JobCollect ----+-> JobPreReport --->   (...)   +-------> JobReport (by JobTranslator)
                                        /                                 \------> StrReport (by JobTranslator)
                          StageCollect ----> StagePreReport ->   (...)   +-------> StageReport (by StageTranslator)
                                                                         \-------> StrReport (by StageTranslator)
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
# TODOs

- [ ] Add diagram to explain basic architecture
- [ ] Use it in PRD projects (json2star, snowflake-push, ...)
- [ ] Improve README to include comparison with other similar solution
- [ ] Add test case with a join
- [ ] Any glass applies to any Report (could it be a performance issue if 100 glasses passed?)
- [ ] This project must be dependency-free, so there is a fix to do on the logger library
- [ ] Review the CappedMap (JMH benchmark?)
- [ ] Add the missing link of SQL queries with children SQL queries
- [ ] Add the missing link SQL queries with stages
- [ ] Investigate if possible to persist SparkUI logs as a complementary approach with
```
From https://docs.databricks.com/en/clusters/configure.html#cluster-log-delivery. 
For example, if the log path is dbfs:/cluster-logs, the log files for a specific cluster will be 
stored in dbfs:/cluster-logs/<cluster-name> and the individual event logs will be stored 
in dbfs:/cluster-logs/<cluster-name>/eventlog/<cluster-name-cluster-ip>/<log-id>/.
Settings:
- spark.eventLog.enabled true
- spark.eventLog.dir dbfs:/databricks/unravel/eventLogs/
```
