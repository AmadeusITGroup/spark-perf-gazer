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
    Change the version to the latest release.

## Next Steps

- [User Guide](user_guide.md) - Full setup, configuration options, and data analysis
- [Contributor Guide](contributor_guide.md) - Build instructions and development setup