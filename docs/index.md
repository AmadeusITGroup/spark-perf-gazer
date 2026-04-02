# PerfGazer

Performance Gazer for Apache Spark.

PerfGazer is a configurable Spark Listener that allows you to retrieve important stats about Spark SQL queries, jobs, and stages in a post-mortem way.

## Features

- Post-mortem analysis of Spark SQL queries, jobs, and stages
- Measure accumulated in-executor durations
- Identify jobs with the longest cumulated execution time
- Detect Spark jobs that have spill
- Monitor SQL metrics (files read, pruned, etc.)
- Investigate predicate pushdowns and their effectiveness
- Connect to any monitoring system

## Why PerfGazer?

The Spark UI has limitations:

- Manual process (UI navigation)
- Often slow to load
- Limited retention (stats data is often purged)
- Not made for analytics

PerfGazer solves these problems by providing programmatic access to execution statistics.

## Next Steps

- [Getting Started](getting-started.md) - Quick start guide
- [User Guide](user_guide/index.md) - Full setup and configuration
- [Contributor Guide](contributor_guide.md) - Build and development
