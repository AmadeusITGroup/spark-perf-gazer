# Analyze using SQL

## Create PerfGazer views 

PerfGazer exposes SQL queries (called `snippets`) to create temporary views to access the PerfGazer data produced by the Spark application. 
You can run those snippets to perform analytics on the SQL queries, jobs, etc.

Within the Spark application, you can access such snippets by doing: 

```scala
import com.amadeus.perfgazer.PerfGazer
val perfGazer = PerfGazer.instance.getOrElse(throw new RuntimeException("Oups"))

val snippets: Set[String] = perfGazer.getSnippets
// snippets.foreach(println) // print them
// snippets.foreach(spark.sql) // launch them
```

Additionally, at Spark application shutdown, PerfGazer will display those snippets in the logs (info log level). You can copy and paste them in a notebook to start performing investigations. 

```sql
-- Copy and paste the snippets shown in the logs by Perfgazer during shutdown (info level)
CREATE OR REPLACE TEMPORARY VIEW sql ...
CREATE OR REPLACE TEMPORARY VIEW job ...
CREATE OR REPLACE TEMPORARY VIEW stage ...
CREATE OR REPLACE TEMPORARY VIEW task ...

```

## Analyze PerfGazer data

Example: deep dive into all tasks and display info of their corresponding parent stage + job

```sql
SELECT *
  FROM job j
  JOIN stage s ON ARRAY_CONTAINS(j.stages, s.stageId)
  JOIN task t ON t.stageId = s.stageId;
```
