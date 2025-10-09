package com.amadeus.sparklear.reports

import com.amadeus.sparklear.entities.StageEntity

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
) extends Report

object StageReport extends Translator[StageEntity, StageReport] {

  def fromEntityToReport(r: StageEntity): StageReport = {
    val p = r.end
    StageReport(
      stageId = r.end.stageInfo.stageId,
      stageSubmissionTime = r.end.stageInfo.submissionTime,
      stageCompletionTime = r.end.stageInfo.completionTime,
      readBytes = p.inputReadBytes,
      writeBytes = p.outputWriteBytes,
      shuffleReadBytes = p.shuffleReadBytes,
      shuffleWriteBytes = p.shuffleWriteBytes,
      execCpuNs = p.execCpuNs,
      execRunNs = p.execRunNs,
      execJvmGcNs = p.execjvmGCNs,
      attempt = p.attempt,
      memoryBytesSpilled = r.end.memoryBytesSpilled,
      diskBytesSpilled = r.end.diskBytesSpilled
    )
  }
}