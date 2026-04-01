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

See the [User Guide](docs/user_guide.md) for setup and usage instructions.

---
## Contributor Guide

See the [Contributor Guide](docs/contributor_guide.md) for technical overview, build instructions, and development setup.

## Authors

- Mauricio JOST
- Generoso PAGANO
- Bruno JOUBERT
- Thierry ACCART
- Sergei DOLGOV
- Mathieu TRAMPONT



