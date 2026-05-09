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

  /** Extracts all scan filter predicates and locations from SQL execution plan details.
    * Returns one row per SQL execution that contains Scan parquet leaf nodes, with arrays
    * of all locations, partition filters, pushed filters, and data filters found in the plan.
    * Requires Spark 3.4+ (REGEXP_EXTRACT_ALL).
    */
  val FiltersPerScan: String =
    """SELECT sq.sqlId,
      |       sq.description,
      |       REGEXP_EXTRACT_ALL(sq.details, 'Location: [^\\[]*\\[([^\\]]*?)(?:\\]|\\.\\.\\.,)', 1) AS locations,
      |       REGEXP_EXTRACT_ALL(sq.details, 'PartitionFilters: \\[([^\\]]*?)(?:\\]|\\.\\.\\.,)', 1) AS partitionFilters,
      |       REGEXP_EXTRACT_ALL(sq.details, 'PushedFilters: \\[([^\\]]*?)(?:\\]|\\.\\.\\.,)', 1) AS pushedFilters,
      |       REGEXP_EXTRACT_ALL(sq.details, 'DataFilters: \\[([^\\]]*?)(?:\\]|\\.\\.\\.,)', 1) AS dataFilters
      |  FROM sql sq
      | WHERE SIZE(FILTER(sq.nodes, n -> n.nodeName LIKE '%Scan parquet%' AND n.isLeaf = true)) > 0
      | ORDER BY sq.sqlId""".stripMargin
}
