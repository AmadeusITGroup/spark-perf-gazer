package com.amadeus.perfgazer.events

import org.apache.spark.scheduler.StageInfo

/** Raw event proving information about a stage
  */
case class StageEvent(
  stageId: Int,
  stageSubmissionTime: Option[Long],
  stageCompletionTime: Option[Long],
  memoryBytesSpilled: Long,
  diskBytesSpilled: Long,
  inputReadBytes: Long,
  outputWriteBytes: Long,
  shuffleReadBytes: Long,
  shuffleWriteBytes: Long,
  execCpuNs: Long,
  execRunNs: Long,
  execjvmGCNs: Long,
  attempt: Int
)

object StageEvent {
  def apply(stageInfo: StageInfo): StageEvent = StageEvent(
    stageId = stageInfo.stageId,
    stageSubmissionTime = stageInfo.submissionTime,
    stageCompletionTime = stageInfo.completionTime,
    memoryBytesSpilled = stageInfo.taskMetrics.memoryBytesSpilled,
    diskBytesSpilled = stageInfo.taskMetrics.diskBytesSpilled,
    inputReadBytes = stageInfo.taskMetrics.inputMetrics.bytesRead,
    outputWriteBytes = stageInfo.taskMetrics.outputMetrics.bytesWritten,
    shuffleReadBytes = stageInfo.taskMetrics.shuffleReadMetrics.totalBytesRead,
    shuffleWriteBytes = stageInfo.taskMetrics.shuffleWriteMetrics.bytesWritten,
    execCpuNs = stageInfo.taskMetrics.executorCpuTime,
    execRunNs = stageInfo.taskMetrics.executorRunTime,
    execjvmGCNs = stageInfo.taskMetrics.jvmGCTime,
    attempt = stageInfo.attemptNumber()
  )
}
