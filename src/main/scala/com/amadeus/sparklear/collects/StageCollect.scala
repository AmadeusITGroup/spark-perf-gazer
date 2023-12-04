package com.amadeus.sparklear.collects

import com.amadeus.sparklear.input.StageInput
import org.apache.spark.scheduler.StageInfo

case class StageCollect(stageInfo: StageInfo) extends Collect[StageInput] {
  val spillMb = if (stageInfo.taskMetrics.memoryBytesSpilled > 0) {
    Some(stageInfo.taskMetrics.memoryBytesSpilled / 1024 / 1024)
  } else {
    None
  }
  val inputReadMb = stageInfo.taskMetrics.inputMetrics.bytesRead / 1024 / 1024
  val outputWriteMb = stageInfo.taskMetrics.outputMetrics.bytesWritten / 1024 / 1024
  val shuffleReadMb = stageInfo.taskMetrics.shuffleReadMetrics.totalBytesRead / 1024 / 1024
  val shuffleWriteMb = stageInfo.taskMetrics.shuffleWriteMetrics.bytesWritten / 1024 / 1024
  val execCpuSecs = stageInfo.taskMetrics.executorCpuTime / 1024 / 1024 / 1024
  val attempt = stageInfo.attemptNumber()
}
