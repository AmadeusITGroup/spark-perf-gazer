package com.amadeus.sparklear.events

import com.amadeus.sparklear.entities.StageEntity
import org.apache.spark.scheduler.StageInfo

/** Raw event proving information about a stage
  */
case class StageEvent(stageInfo: StageInfo) extends Event[StageEntity] {
  val memoryBytesSpilled: Long = stageInfo.taskMetrics.memoryBytesSpilled
  val diskBytesSpilled: Long = stageInfo.taskMetrics.diskBytesSpilled
  val inputReadBytes: Long = stageInfo.taskMetrics.inputMetrics.bytesRead
  val outputWriteBytes: Long = stageInfo.taskMetrics.outputMetrics.bytesWritten
  val shuffleReadBytes: Long = stageInfo.taskMetrics.shuffleReadMetrics.totalBytesRead
  val shuffleWriteBytes: Long = stageInfo.taskMetrics.shuffleWriteMetrics.bytesWritten
  val execCpuNs: Long = stageInfo.taskMetrics.executorCpuTime
  val execRunNs: Long = stageInfo.taskMetrics.executorRunTime
  val execjvmGCNs: Long = stageInfo.taskMetrics.jvmGCTime
  val attempt: Int = stageInfo.attemptNumber()
}
