package com.amadeus.perfgazer

/**
  * Collection of SQL queries for analyzing PerfGazer reports.
  *
  * These queries are the definitive source of truth for the analysis use cases
  * documented in docs/user_guide/analyze_sql.md.
  *
  * They assume the PerfGazer JSON report views (sql, job, stage, task) have been
  * created via the snippets provided by [[PerfGazer.getSnippets]].
  */
object AnalysisQueries {

  /** Aggregates executor CPU time across all stages of each job, converted from nanoseconds to seconds. */
  val JobsByCpuUsage: String =
    """SELECT j.jobId,
      |       j.jobName,
      |       ROUND(SUM(s.execCpuNs) / 1e9, 2) AS cpuTimeSec
      |  FROM job j
      |  JOIN stage s ON ARRAY_CONTAINS(j.stages, s.stageId)
      | GROUP BY j.jobId, j.jobName
      | ORDER BY cpuTimeSec DESC""".stripMargin

  /** Shows input, output, shuffle read/write and total I/O per job, all in MB. */
  val JobsByIoVolumes: String =
    """SELECT j.jobId,
      |       j.jobName,
      |       ROUND(SUM(s.readBytes)         / 1048576, 2) AS inputMb,
      |       ROUND(SUM(s.writeBytes)        / 1048576, 2) AS outputMb,
      |       ROUND(SUM(s.shuffleReadBytes)  / 1048576, 2) AS shuffleReadMb,
      |       ROUND(SUM(s.shuffleWriteBytes) / 1048576, 2) AS shuffleWriteMb,
      |       ROUND(SUM(s.readBytes + s.writeBytes
      |               + s.shuffleReadBytes + s.shuffleWriteBytes) / 1048576, 2) AS totalIoMb
      |  FROM job j
      |  JOIN stage s ON ARRAY_CONTAINS(j.stages, s.stageId)
      | GROUP BY j.jobId, j.jobName
      | ORDER BY totalIoMb DESC""".stripMargin

  /** Lists only jobs where memory or disk spill occurred, in MB. */
  val JobsWithSpill: String =
    """SELECT j.jobId,
      |       j.jobName,
      |       ROUND(SUM(s.memoryBytesSpilled) / 1048576, 2) AS memorySpillMb,
      |       ROUND(SUM(s.diskBytesSpilled)   / 1048576, 2) AS diskSpillMb
      |  FROM job j
      |  JOIN stage s ON ARRAY_CONTAINS(j.stages, s.stageId)
      | GROUP BY j.jobId, j.jobName
      |HAVING SUM(s.memoryBytesSpilled) > 0
      |    OR SUM(s.diskBytesSpilled)   > 0
      | ORDER BY diskSpillMb DESC""".stripMargin

  /** Computes the elapsed wall-clock time of each job in seconds. */
  val WallClockDurationOfJobs: String =
    """SELECT j.jobId,
      |       j.jobName,
      |       ROUND((j.jobEndTime - j.jobStartTime) / 1000, 2) AS wallClockSec
      |  FROM job j
      | ORDER BY wallClockSec DESC""".stripMargin

  /** Explodes SQL plan nodes and returns metrics for join operators. */
  val JoinNodeMetrics: String =
    """SELECT sqlId,
      |       node.nodeName,
      |       node.jobName,
      |       FROM_JSON(TO_JSON(node.metrics), 'MAP<STRING, STRING>') AS metrics
      |  FROM (SELECT sqlId, EXPLODE(nodes) AS node FROM sql) subquery
      | WHERE node.nodeName LIKE '%Join%'""".stripMargin

  /** Explodes SQL plan nodes and returns metrics for scan parquet operators. */
  val ScanNodeMetrics: String =
    """SELECT sqlId,
      |       node.nodeName,
      |       node.jobName,
      |       FROM_JSON(TO_JSON(node.metrics), 'MAP<STRING, STRING>') AS metrics
      |  FROM (SELECT sqlId, EXPLODE(nodes) AS node FROM sql) subquery
      | WHERE node.nodeName LIKE '%Scan parquet%'""".stripMargin

  /** Detects task-level skew per job/stage using statistical thresholds.
    *
    * Reports stages where the maximum task duration exceeds 1.5x the 75th percentile,
    * indicating that a few tasks are significantly slower than the rest.
    */
  val SkewDetection: String =
    """SELECT j.jobId,
      |       j.jobName,
      |       t.stageId,
      |       COUNT(1) AS taskCount,
      |       ROUND(MAX(t.executorRunTime) / 1000, 2) AS maxDurationSec,
      |       ROUND(PERCENTILE(t.executorRunTime, 0.5) / 1000, 2) AS medianDurationSec,
      |       ROUND(PERCENTILE(t.executorRunTime, 0.75) / 1000, 2) AS p75DurationSec,
      |       ROUND(STDDEV(t.executorRunTime) / 1000, 2) AS stddevDurationSec,
      |       ROUND(MAX(t.executorRunTime) / PERCENTILE(t.executorRunTime, 0.75), 2) AS skewFactor
      |  FROM job j
      |  JOIN stage s ON ARRAY_CONTAINS(j.stages, s.stageId)
      |  JOIN task t ON t.stageId = s.stageId
      | GROUP BY j.jobId, j.jobName, t.stageId
      |HAVING MAX(t.executorRunTime) > 1.5 * PERCENTILE(t.executorRunTime, 0.75)
      | ORDER BY skewFactor DESC""".stripMargin

}
