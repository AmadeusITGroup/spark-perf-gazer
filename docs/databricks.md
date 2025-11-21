# Databricks setup

You will need to install the library according to the Databricks runtime being used.

| Runtime version | Spark version | Installation method                |
|-----------------|---------------|------------------------------------|
| 13.3            | Spark 3.4.1   | Upload and install from DBFS       |
| 16.4            | Spark 3.5.2   | Upload and install from Workspace  |

To install the library on job clusters you can follow these steps:

- 1. Upload the PerfGazer JAR to a location accessible by the job cluster (e.g., DBFS).  
- 2. You can use an init script to install the PerfGazer library on the job cluster, for example:

```shell
if [ -f "/databricks/jars/perfgazer_spark_3-5-2_2_12_0_0_29_SNAPSHOT.jar" ]; then
  rm -f "/databricks/jars/perfgazer_spark_3-5-2_2_12_0_0_29_SNAPSHOT.jar"
fi

cp -f /dbfs/FileStore/jars/perfgazer_spark_3-5-2_2_12_0_0_29_SNAPSHOT.jar /databricks/jars
```
