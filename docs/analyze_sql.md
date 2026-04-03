# Analyze using SQL

At application shutdown, PerfGazer prints view creation snippets in the logs that match your configuration. Run those snippets to create temporary views, then query them.

If you set up PerfGazer programatically, you can access such snippets by doing: 

```scala
val perfGazer = ...
// spark application execution
perfGazer.close()

val snippets: Set[String] = perfGazer.getSnippets
snippets.foreach(println)
```

```sql
-- Run the generated snippets to create the views
CREATE OR REPLACE TEMPORARY VIEW sql ...
CREATE OR REPLACE TEMPORARY VIEW job ...
CREATE OR REPLACE TEMPORARY VIEW stage ...
CREATE OR REPLACE TEMPORARY VIEW task ...

-- Example: join jobs, stages, and tasks
SELECT *
  FROM job j
  JOIN stage s ON s.applicationId = j.applicationId AND ARRAY_CONTAINS(j.stages, s.stageId)
  JOIN task t ON t.applicationId = s.applicationId AND t.stageId = s.stageId;
```

NOTE: it is possible to create views that allow to compare different runs by reworking the above views, so that they point to the base path instead of specific fixed partitions.