# Analyze using SQL

## Create PerfGazer views 

PerfGazer exposes SQL queries (called `snippets`) to create temporary views to access the PerfGazer data produced by the Spark application. 
You can run those snippets to perform analytics on the SQL queries, jobs, etc.

Within the Spark application, you can access such snippets by doing: 

```scala
import com.amadeus.perfgazer.PerfGazer
val perfGazer = PerfGazer.instance.getOrElse(throw new RuntimeException("Oops"))

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

## Query across all runs

The snippets above point to the current run. To create a view spanning all runs available, you can use `**` with a `basePath`. 
For example:

```sql
CREATE OR REPLACE TEMPORARY VIEW [sql|job|stage|...]
USING json
OPTIONS (
  path "<base_path>/**/[sql|job|stage|...]-reports-*.json",
  basePath "<base_path>/"
);
```

Replace `<base_path>` with your actual base destination. 

The `basePath` option indicates Spark from which point start performing auto-discover of partition columns (e.g. `applicationId`).

Mind that if you use `basePath` and new partitions are discovered, the joins between the views will have to take into account partition columns if meaningful to associate correctly jobs/stages/... from different runs.

## Analyze PerfGazer data

The SQL queries below are available as constants in `com.amadeus.perfgazer.AnalysisQueries`. That class is the definitive source of truth for these queries and is tested in the integration test suite.

You can start deep diving into all tasks with their parent stage and job with a query like the following:

```sql
SELECT *
  FROM job j
  JOIN stage s ON ARRAY_CONTAINS(j.stages, s.stageId)
  JOIN task t ON t.stageId = s.stageId;
```

Below we provide a collection of queries you can run to explore various performance aspects of your Spark application.

### Jobs by CPU usage

Aggregates executor CPU time across all stages of each job, converted from nanoseconds to seconds.

Code reference: `AnalysisQueries.JobsByCpuUsage`

```sql
SELECT j.jobId,
       j.jobName,
       ROUND(SUM(s.execCpuNs) / 1e9, 2) AS cpuTimeSec
  FROM job j
  JOIN stage s ON ARRAY_CONTAINS(j.stages, s.stageId)
 GROUP BY j.jobId, j.jobName
 ORDER BY cpuTimeSec DESC;
```

??? example "Sample output"

    | jobId | jobName              | cpuTimeSec |
    |------:|----------------------|-----------:|
    |     2 | save at MyApp.scala:42 |    1782.43 |
    |     1 | count at MyApp.scala:28 |     624.18 |
    |     0 | read at MyApp.scala:15  |      84.92 |

### Jobs by I/O volumes

Shows input, output, shuffle read/write and total I/O per job, all in MB.

Code reference: `AnalysisQueries.JobsByIoVolumes`

```sql
SELECT j.jobId,
       j.jobName,
       ROUND(SUM(s.readBytes)         / 1048576, 2) AS inputMb,
       ROUND(SUM(s.writeBytes)        / 1048576, 2) AS outputMb,
       ROUND(SUM(s.shuffleReadBytes)  / 1048576, 2) AS shuffleReadMb,
       ROUND(SUM(s.shuffleWriteBytes) / 1048576, 2) AS shuffleWriteMb,
       ROUND(SUM(s.readBytes + s.writeBytes
               + s.shuffleReadBytes + s.shuffleWriteBytes) / 1048576, 2) AS totalIoMb
  FROM job j
  JOIN stage s ON ARRAY_CONTAINS(j.stages, s.stageId)
 GROUP BY j.jobId, j.jobName
 ORDER BY totalIoMb DESC;
```

??? example "Sample output"

    | jobId | jobName              | inputMb | outputMb | shuffleReadMb | shuffleWriteMb | totalIoMb |
    |------:|----------------------|--------:|---------:|--------------:|---------------:|----------:|
    |     2 | save at MyApp.scala:42 | 1024.00 |   512.34 |        256.78 |         248.91 |   2042.03 |
    |     1 | count at MyApp.scala:28 |  512.00 |     0.00 |        128.45 |         130.12 |    770.57 |
    |     0 | read at MyApp.scala:15  |  256.00 |     0.00 |          0.00 |           0.00 |    256.00 |

### Jobs with spill

Lists only jobs where memory or disk spill occurred, in MB.

Code reference: `AnalysisQueries.JobsWithSpill`

```sql
SELECT j.jobId,
       j.jobName,
       ROUND(SUM(s.memoryBytesSpilled) / 1048576, 2) AS memorySpillMb,
       ROUND(SUM(s.diskBytesSpilled)   / 1048576, 2) AS diskSpillMb
  FROM job j
  JOIN stage s ON ARRAY_CONTAINS(j.stages, s.stageId)
 GROUP BY j.jobId, j.jobName
HAVING SUM(s.memoryBytesSpilled) > 0
    OR SUM(s.diskBytesSpilled)   > 0
 ORDER BY diskSpillMb DESC;
```

??? example "Sample output"

    | jobId | jobName              | memorySpillMb | diskSpillMb |
    |------:|----------------------|--------------:|------------:|
    |     2 | save at MyApp.scala:42 |       2048.00 |      384.56 |
    |     1 | count at MyApp.scala:28 |        512.00 |       64.12 |

### All joins with CPU and I/O from their job

Explodes the SQL plan nodes to find join operators, then enriches them with the aggregated CPU time and I/O volumes of the parent job.

```sql
WITH job_stats AS (
  SELECT j.jobId,
         j.jobName,
         j.sqlId,
         ROUND(SUM(s.execCpuNs) / 1e9, 2) AS cpuTimeSec,
         ROUND(SUM(s.readBytes + s.writeBytes
                 + s.shuffleReadBytes + s.shuffleWriteBytes) / 1048576, 2) AS totalIoMb
    FROM job j
    JOIN stage s ON ARRAY_CONTAINS(j.stages, s.stageId)
   GROUP BY j.jobId, j.jobName, j.sqlId
)
SELECT sq.sqlId,
       sq.description,
       n.nodeName   AS joinNode,
       js.jobId,
       js.cpuTimeSec,
       js.totalIoMb
  FROM sql sq
  JOIN job_stats js ON js.sqlId = CAST(sq.sqlId AS STRING)
       LATERAL VIEW EXPLODE(sq.nodes) AS n
 WHERE n.nodeName LIKE '%Join%'
 ORDER BY js.cpuTimeSec DESC;
```

??? example "Sample output"

    | sqlId | description          | joinNode          | jobId | cpuTimeSec | totalIoMb |
    |------:|----------------------|-------------------|------:|-----------:|----------:|
    |     2 | Join orders with customers | SortMergeJoin     |     2 |     124.57 |   2042.03 |
    |     1 | Enrich transactions  | BroadcastHashJoin |     1 |      58.23 |    770.57 |
    |     0 | Aggregate daily totals | ShuffledHashJoin  |     0 |      32.11 |    256.00 |

### Join node metrics

Explodes SQL plan nodes and returns metrics for join operators. Useful for inspecting the number of output rows produced by each join.

Code reference: `AnalysisQueries.JoinNodeMetrics`

```sql
SELECT sqlId,
       node.nodeName,
       node.jobName,
       FROM_JSON(TO_JSON(node.metrics), 'MAP<STRING, STRING>') AS metrics
  FROM (SELECT sqlId, EXPLODE(nodes) AS node FROM sql) subquery
 WHERE node.nodeName LIKE '%Join%';
```

??? example "Sample output"

    | sqlId | nodeName          | jobName | metrics                          |
    |------:|-------------------|---------|----------------------------------|
    |     1 | BroadcastHashJoin | jobjoin | {number of output rows -> 2}     |
    |     2 | SortMergeJoin     | bigjoin | {number of output rows -> 10000} |

### Scan node metrics

Explodes SQL plan nodes and returns metrics for scan parquet operators. Useful for checking how many files and rows were read by each scan.

Code reference: `AnalysisQueries.ScanNodeMetrics`

```sql
SELECT sqlId,
       node.nodeName,
       node.jobName,
       FROM_JSON(TO_JSON(node.metrics), 'MAP<STRING, STRING>') AS metrics
  FROM (SELECT sqlId, EXPLODE(nodes) AS node FROM sql) subquery
 WHERE node.nodeName LIKE '%Scan parquet%';
```

??? example "Sample output"

    | sqlId | nodeName                    | jobName | metrics                                                        |
    |------:|-----------------------------|---------|----------------------------------------------------------------|
    |     1 | Scan parquet delta.`/path`  | jobjoin | {number of files read -> 1, number of output rows -> 252}      |
    |     1 | Scan parquet delta.`/path2` | jobjoin | {number of files read -> 4, number of output rows -> 9000}     |

### Wall clock duration of jobs

Computes the elapsed wall-clock time of each job in seconds.

Code reference: `AnalysisQueries.WallClockDurationOfJobs`

```sql
SELECT j.jobId,
       j.jobName,
       ROUND((j.jobEndTime - j.jobStartTime) / 1000, 2) AS wallClockSec
  FROM job j
 ORDER BY wallClockSec DESC;
```

??? example "Sample output"

    | jobId | jobName              | wallClockSec |
    |------:|----------------------|-------------:|
    |     2 | save at MyApp.scala:42 |       245.67 |
    |     1 | count at MyApp.scala:28 |        98.34 |
    |     0 | read at MyApp.scala:15  |        15.21 |

### Skew detection

Detects task-level skew per job/stage using statistical thresholds. Reports stages where the maximum task duration exceeds 1.5x the 75th percentile, indicating that a few tasks are significantly slower than the rest. The `skewFactor` column quantifies how much the slowest task deviates from the pack.

Code reference: `AnalysisQueries.SkewDetection`

```sql
SELECT j.jobId,
       j.jobName,
       t.stageId,
       COUNT(1) AS taskCount,
       ROUND(MAX(t.executorRunTime) / 1000, 2) AS maxDurationSec,
       ROUND(PERCENTILE(t.executorRunTime, 0.5) / 1000, 2) AS medianDurationSec,
       ROUND(PERCENTILE(t.executorRunTime, 0.75) / 1000, 2) AS p75DurationSec,
       ROUND(STDDEV(t.executorRunTime) / 1000, 2) AS stddevDurationSec,
       ROUND(MAX(t.executorRunTime) / PERCENTILE(t.executorRunTime, 0.75), 2) AS skewFactor
  FROM job j
  JOIN stage s ON ARRAY_CONTAINS(j.stages, s.stageId)
  JOIN task t ON t.stageId = s.stageId
 GROUP BY j.jobId, j.jobName, t.stageId
HAVING MAX(t.executorRunTime) > 1.5 * PERCENTILE(t.executorRunTime, 0.75)
 ORDER BY skewFactor DESC;
```

??? example "Sample output"

    | jobId | jobName              | stageId | taskCount | maxDurationSec | medianDurationSec | p75DurationSec | stddevDurationSec | skewFactor |
    |------:|----------------------|--------:|----------:|---------------:|------------------:|---------------:|------------------:|-----------:|
    |     2 | save at MyApp.scala:42 |       3 |       200 |          45.20 |              2.10 |           3.50 |             8.42 |      12.91 |
    |     1 | count at MyApp.scala:28 |       1 |       100 |          12.80 |              1.50 |           2.00 |             3.21 |       6.40 |

