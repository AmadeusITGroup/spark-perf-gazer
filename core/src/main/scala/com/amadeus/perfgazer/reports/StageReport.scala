package com.amadeus.perfgazer.reports

import com.amadeus.perfgazer.events.StageEvent
import com.amadeus.perfgazer.schema.{SchemaDoc, SchemaReport}

@SchemaReport("stage", "Stage-level execution report. One row per completed Spark stage.")
case class StageReport(
  @SchemaDoc("Unique stage identifier")
  stageId: Int,
  @SchemaDoc("Epoch timestamp when the stage was submitted", unit = "ms")
  stageSubmissionTime: Option[Long],
  @SchemaDoc("Epoch timestamp when the stage completed", unit = "ms")
  stageCompletionTime: Option[Long],
  @SchemaDoc("Total input bytes read", unit = "bytes")
  readBytes: Long,
  @SchemaDoc("Total output bytes written", unit = "bytes")
  writeBytes: Long,
  @SchemaDoc("Total shuffle bytes read", unit = "bytes")
  shuffleReadBytes: Long,
  @SchemaDoc("Total shuffle bytes written", unit = "bytes")
  shuffleWriteBytes: Long,
  @SchemaDoc("Executor CPU time", unit = "ns")
  execCpuNs: Long,
  @SchemaDoc("Executor run time", unit = "ns")
  execRunNs: Long,
  @SchemaDoc("Executor JVM garbage collection time", unit = "ns")
  execJvmGcNs: Long,
  @SchemaDoc("Stage attempt number")
  attempt: Int,
  @SchemaDoc("Bytes spilled to memory", unit = "bytes")
  memoryBytesSpilled: Long,
  @SchemaDoc("Bytes spilled to disk", unit = "bytes")
  diskBytesSpilled: Long
) extends Report {
  override def reportType: ReportType = StageReportType
}

object StageReport {

  /** Create a StageReport
    *
    * @param end the StageEvent for stage end
    * @return the StageReport generated
    */
  def apply(end: StageEvent): StageReport = {
    StageReport(
      stageId = end.stageId,
      stageSubmissionTime = end.stageSubmissionTime,
      stageCompletionTime = end.stageCompletionTime,
      readBytes = end.inputReadBytes,
      writeBytes = end.outputWriteBytes,
      shuffleReadBytes = end.shuffleReadBytes,
      shuffleWriteBytes = end.shuffleWriteBytes,
      execCpuNs = end.execCpuNs,
      execRunNs = end.execRunNs,
      execJvmGcNs = end.execjvmGCNs,
      attempt = end.attempt,
      memoryBytesSpilled = end.memoryBytesSpilled,
      diskBytesSpilled = end.diskBytesSpilled
    )
  }

}
