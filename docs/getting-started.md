# Getting Started

## Quick Start

The fastest way to try PerfGazer is via `spark-shell`:

```bash
spark-shell \
  --packages io.github.amadeusitgroup:perfgazer_spark_3-5-2_2.12:0.0.1 \
  --conf spark.extraListeners=com.amadeus.perfgazer.PerfGazer \
  --conf spark.perfgazer.sink.class=com.amadeus.perfgazer.JsonSink \
  --conf spark.perfgazer.sink.json.destination=/tmp/perfgazer/output
```

!!! note
    Change the version to the latest release: ![GitHub Release](https://img.shields.io/github/v/release/AmadeusITGroup/spark-perf-gazer)

Run some Spark actions:

```scala
spark.range(1000000).groupBy("id").count().collect()
```

Then explore the generated reports:

```bash
ls /tmp/perfgazer/output/
# job-reports-*.json, stage-reports-*.json, sql-reports-*.json
```

You can now query them directly in Spark (example for job reports):

```sql
CREATE OR REPLACE TEMPORARY VIEW job
USING json
OPTIONS (path '/tmp/perfgazer/output/job-reports-*.json');

SELECT * FROM job;
```

## Next Steps

- [User Guide](user_guide.md) - Full setup, configuration options, and data analysis
- [Contributor Guide](contributor_guide.md) - Build instructions and development setup