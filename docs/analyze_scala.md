# Analyze using Scala

Read the JSON files produced by `JsonSink` directly into Spark DataFrames.

```scala
import org.apache.spark.sql.functions._

val sparkPath = "dbfs:/perfgazer/jsonsink/"

val dfJobsReports   = spark.read.option("basePath", sparkPath).json(sparkPath + "clusterName=*/date=*/applicationId=*/job-reports-*.json")
val dfStagesReports = spark.read.option("basePath", sparkPath).json(sparkPath + "clusterName=*/date=*/applicationId=*/stage-reports-*.json")
val dfTasksReports  = spark.read.option("basePath", sparkPath).json(sparkPath + "clusterName=*/date=*/applicationId=*/task-reports-*.json")

// Reconcile reports across jobs, stages, and tasks
val dfTasks = dfJobsReports
  .withColumn("stageId", explode(col("stages")))
  .drop("stages")
  .join(dfStagesReports, Seq("date", "applicationId", "stageId"))
  .join(dfTasksReports,  Seq("date", "applicationId", "stageId"))

display(dfTasks)
```
