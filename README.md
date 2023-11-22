# SPARKLEAR

This repository contains the SparklEar Spark Listener.

SparklEar is a configurable Spark Listener that allows to retrieve important stats about Spark SQL queries and jobs, post-mortem.
Users should attach it as a listener to the `SparkSession`. It will cause to report certain lines in the logs, which can be
interpreted afterwards.

The use-cases that this library is intended to address:

- allow to do post-mortem analysis of SQL queries, jobs and stages programmatically
- measure Spark jobs accumulated in-executor durations
- identify jobs that take the longer cumulated execution time (as measured in executors)
- identify Spark jobs that have spill
- monitor certain SQL metrics like amount of files read
- ...

Why not using the regular Spark UI? 
There are some problems with the analysis of execution stats from the Spark UI:
- the process is mostly manual
- it is often slow (takes time to load)
- has a limited retention (so stats data is often loss for large applications)

https://docs.databricks.com/en/_extras/notebooks/source/kb/clusters/event-log-replay.html


## Developers

Using `sbt` you can run tests locally: `sbt test`

### Intellij
Using IntelliJ IDEA, you can update the ScalaTest Configuration Template to avoid manual settings.

```
Go to Run -> Edit Configurations -> Edit configuration templates -> ScalaTest 
```

For code formatting setup: 

```
Settings -> Editor -> Code Style -> Scala -> Formatter: ScalaFMT
```
# Todo

// TODO make clear with the type names at which level we are
// SqlWrapper => put in Map, allows to build SqlSummary
// JobWrapper => put in Map, allows to build JobSummary
// SqlSummary => thing that becomes json
// JobSummary => thing that becomes json
// SparkStuffInitial => Wrapper in map => end + Metrics resolution + Wrapper = Summary

