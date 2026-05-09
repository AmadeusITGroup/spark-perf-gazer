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

### Wall clock duration of jobs

Computes the elapsed wall-clock time of each job in seconds.

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

### Delta tables read with their scan filters

Each Scan parquet node in the physical plan includes a table location and three types of filter predicates:

- **Location**: the path to the table or file index being scanned.
- **PartitionFilters**: predicates on partition columns, used to prune entire partition directories before reading.
- **PushedFilters**: predicates pushed down to the Parquet reader, used to skip row groups via file statistics.
- **DataFilters**: predicates applied row-by-row after reading, for expressions that could not be pushed down.

This query extracts all occurrences of each field as independent arrays — one per SQL execution. This avoids positional correlation issues that can arise when different fields appear a different number of times in the plan text (Spark 3.4+):

```sql
SELECT sq.sqlId,
       sq.description,
       REGEXP_EXTRACT_ALL(sq.details, 'Location: [^\\[]*\\[([^\\]]*?)(?:\\]|\\.\\.\\.,)', 1) AS locations,
       REGEXP_EXTRACT_ALL(sq.details, 'PartitionFilters: \\[([^\\]]*)\\]', 1) AS partitionFilters,
       REGEXP_EXTRACT_ALL(sq.details, 'PushedFilters: \\[([^\\]]*)\\]', 1) AS pushedFilters,
       REGEXP_EXTRACT_ALL(sq.details, 'DataFilters: \\[([^\\]]*)\\]', 1) AS dataFilters,
       sq.details
  FROM sql sq
 WHERE SIZE(FILTER(sq.nodes, n -> n.nodeName LIKE '%Scan parquet%' AND n.isLeaf = true)) > 0
 ORDER BY sq.sqlId;
```

This query is available as `com.amadeus.perfgazer.AnalysisQueries.FiltersPerScan`.

The `details` column is included for debugging when regex extraction produces unexpected results. The Location regex handles both full paths (`Location: ...[path]`) and truncated paths on Databricks (`Location: ...[path...,`).

??? example "Sample output"

    | sqlId | description            | locations                                                    | partitionFilters                          | pushedFilters                                                          | dataFilters                                              |
    |------:|------------------------|--------------------------------------------------------------|-------------------------------------------|------------------------------------------------------------------------|----------------------------------------------------------|
    |     1 | Filter by region       | ["dbfs:/data/warehouse/customers", "dbfs:/data/warehouse/orders"] | ["isnotnull(region), (region = EMEA)", ""] | ["IsNotNull(region), EqualTo(region,EMEA)", ""]                        | ["isnotnull(region), (region = EMEA)", ""]               |
    |     2 | Join orders with items | ["dbfs:/data/warehouse/orders", "dbfs:/data/warehouse/items"]    | ["", ""]                                  | ["IsNotNull(order_date), GreaterThan(order_date,2024-01-01)", ""]      | ["isnotnull(order_date), (order_date > 2024-01-01)", ""] |

