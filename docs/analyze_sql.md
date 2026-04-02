# Analyze using SQL

At application shutdown, PerfGazer prints view creation snippets in the logs that match your configuration. Run those snippets to create temporary views, then query them.

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
