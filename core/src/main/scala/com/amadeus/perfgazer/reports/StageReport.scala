package com.amadeus.perfgazer.reports

import com.amadeus.perfgazer.events.StageEvent
import com.amadeus.perfgazer.schema.{ColumnDoc, TableDoc}

@TableDoc(name = "stage", description = "Stage-level execution report. One row per completed Spark stage.")
case class StageReport(
  @ColumnDoc(description = "Unique stage identifier")
  stageId: Int,
  @ColumnDoc(description = "Epoch timestamp when the stage was submitted", unit = "ms")
  stageSubmissionTime: Option[Long],
  @ColumnDoc(description = "Epoch timestamp when the stage completed", unit = "ms")
  stageCompletionTime: Option[Long],
  @ColumnDoc(description = "Total input bytes read", unit = "bytes")
  readBytes: Long,
  @ColumnDoc(description = "Total output bytes written", unit = "bytes")
  writeBytes: Long,
  @ColumnDoc(description = "Total shuffle bytes read", unit = "bytes")
  shuffleReadBytes: Long,
  @ColumnDoc(description = "Total shuffle bytes written", unit = "bytes")
  shuffleWriteBytes: Long,
  @ColumnDoc(description = "Executor CPU time", unit = "ns")
  execCpuNs: Long,
  @ColumnDoc(description = "Executor run time", unit = "ns")
  execRunNs: Long,
  @ColumnDoc(description = "Executor JVM garbage collection time", unit = "ns")
  execJvmGcNs: Long,
  @ColumnDoc(description = "Stage attempt number")
  attempt: Int,
  @ColumnDoc(description = "Bytes spilled to memory", unit = "bytes")
  memoryBytesSpilled: Long,
  @ColumnDoc(description = "Bytes spilled to disk", unit = "bytes")
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
