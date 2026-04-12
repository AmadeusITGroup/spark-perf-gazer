# User Guide

Follow these steps to get PerfGazer up and running.

## Step 1 — Annotate your Spark code

This step is optional but strongly encouraged as it will make the listener data much easier to analyze, specially with Spark applications with several jobs.

Add job descriptions to your Spark code using `setJobDescription` or `setLocalProperty`:

```scala
spark.sparkContext.setJobDescription("my-etl-job: loading customer data")
```

or with a job group:

```scala
spark.sparkContext.setJobGroup("my-etl-job", "loading customer data")
```

These labels will appear in the collected reports and help you correlate metrics back to specific parts of your application.

## Step 2 — Set up the listener

The listener can be configured in two ways. The default and recommended approach is via Spark properties, which requires no code changes.

- [Configuration via Spark properties](setup_spark_properties.md) ← start here
- [Configuration via code change](setup_code.md)

For Databricks-specific setup, see [Databricks](databricks.md).

## Step 3 — Run your Spark application

Run your application as usual. PerfGazer will collect metrics in the background and flush them to the configured sink.

PerfGazer registers a shutdown hook that ensures the listener is closed gracefully when the driver JVM exits, regardless of which setup method you used.

## Step 4 — Analyze the listener data

Once your application has run, the collected reports can be analyzed in different ways depending on your environment and preference:

- [Analyze using SQL](analyze_sql.md)
- [Analyze using Scala](analyze_scala.md)

> Note: at application shutdown, PerfGazer prints view creation snippets in the logs that match your configuration. These are a convenient starting point for SQL analysis.
