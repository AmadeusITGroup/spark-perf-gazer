# Databricks job cluster setup

To install the library on job clusters you can follow these steps:

- 1. Upload the spark-perf-gazer JAR to a location accessible by the job cluster (e.g., DBFS).
- 2. You can use an init script to install the PerfGazer library on the job cluster, for example:

```shell
cp -f /dbfs/<some_path>/perfgazer_spark_<some_version>.jar /databricks/jars
```

