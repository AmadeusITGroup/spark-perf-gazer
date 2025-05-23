package com.amadeus.sparklear.raw

import com.amadeus.sparklear.prereports.StagePreReport
import org.apache.spark.scheduler.StageInfo

/**
  * Raw event proving information about a stage
  */
case class StageRawEvent(stageInfo: StageInfo) extends RawEvent[StagePreReport] {
  val spillMb = if (stageInfo.taskMetrics.memoryBytesSpilled > 0) {
    Some(stageInfo.taskMetrics.memoryBytesSpilled / 1024 / 1024)
  } else {
    None
  }
  val inputReadMb = stageInfo.taskMetrics.inputMetrics.bytesRead / 1024 / 1024
  val outputWriteMb = stageInfo.taskMetrics.outputMetrics.bytesWritten / 1024 / 1024
  val shuffleReadMb = stageInfo.taskMetrics.shuffleReadMetrics.totalBytesRead / 1024 / 1024
  val shuffleWriteMb = stageInfo.taskMetrics.shuffleWriteMetrics.bytesWritten / 1024 / 1024
  val execCpuSecs = stageInfo.taskMetrics.executorCpuTime / 1000 / 1000 / 1000
  val execRunSecs = stageInfo.taskMetrics.executorRunTime / 1000 / 1000 / 1000
  val execjvmGCSecs = stageInfo.taskMetrics.jvmGCTime / 1000 / 1000 / 1000
  val attempt = stageInfo.attemptNumber()
}
