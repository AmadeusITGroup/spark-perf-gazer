package com.amadeus.perfgazer.reports

import com.amadeus.perfgazer.events.StageEvent

case class StageReport(
  stageId: Int,
  stageSubmissionTime: Option[Long],
  stageCompletionTime: Option[Long],
  readBytes: Long,
  writeBytes: Long,
  shuffleReadBytes: Long,
  shuffleWriteBytes: Long,
  execCpuNs: Long,
  execRunNs: Long,
  execJvmGcNs: Long,
  attempt: Int,
  memoryBytesSpilled: Long,
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
