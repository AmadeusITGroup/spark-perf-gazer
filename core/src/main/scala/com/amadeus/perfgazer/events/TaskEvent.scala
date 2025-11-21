package com.amadeus.perfgazer.events

import org.apache.spark.scheduler.SparkListenerTaskEnd

/** Raw event proving information about a Task
  */
case class TaskEvent(
  stageId: Int,
  taskId: Long,
  taskDuration: Long,
  taskLaunchTime: Long,
  taskFinishTime: Long,
  executorRunTime: Long,
  executorCpuTime: Long,
  executorDeserializeTime: Long,
  executorDeserializeCpuTime: Long,
  resultSize: Long,
  diskBytesSpilled: Long,
  memoryBytesSpilled: Long,
  bytesRead: Long,
  recordsRead: Long,
  jvmGCTime: Long,
  bytesWritten: Long,
  recordsWritten: Long,
  peakExecutionMemory: Long,
  resultSerializationTime: Long,
  fetchWaitTime: Long,
  localBlocksFetched: Long,
  localBytesRead: Long,
  remoteBlocksFetched: Long,
  remoteBytesRead: Long,
  remoteBytesReadToDisk: Long,
  totalRecordsRead: Long,
  remoteRequestsDuration: Long,
  shuffleBytesWritten: Long,
  shuffleRecordsWritten: Long,
  shuffleWriteTime: Long
)

object TaskEvent {
  def apply(taskEnd: SparkListenerTaskEnd): TaskEvent = TaskEvent(
    stageId = taskEnd.stageId,
    taskId = taskEnd.taskInfo.taskId,
    taskDuration = taskEnd.taskInfo.duration,
    taskLaunchTime = taskEnd.taskInfo.launchTime,
    taskFinishTime = taskEnd.taskInfo.finishTime,
    executorRunTime = taskEnd.taskMetrics.executorRunTime,
    executorCpuTime = taskEnd.taskMetrics.executorCpuTime,
    executorDeserializeTime = taskEnd.taskMetrics.executorDeserializeTime,
    executorDeserializeCpuTime = taskEnd.taskMetrics.executorDeserializeCpuTime,
    resultSize = taskEnd.taskMetrics.resultSize,
    diskBytesSpilled = taskEnd.taskMetrics.diskBytesSpilled,
    memoryBytesSpilled = taskEnd.taskMetrics.memoryBytesSpilled,
    bytesRead = taskEnd.taskMetrics.inputMetrics.bytesRead,
    recordsRead = taskEnd.taskMetrics.inputMetrics.recordsRead,
    jvmGCTime = taskEnd.taskMetrics.jvmGCTime,
    bytesWritten = taskEnd.taskMetrics.outputMetrics.bytesWritten,
    recordsWritten = taskEnd.taskMetrics.outputMetrics.recordsWritten,
    peakExecutionMemory = taskEnd.taskMetrics.peakExecutionMemory,
    resultSerializationTime = taskEnd.taskMetrics.resultSerializationTime,
    fetchWaitTime = taskEnd.taskMetrics.shuffleReadMetrics.fetchWaitTime,
    localBlocksFetched = taskEnd.taskMetrics.shuffleReadMetrics.localBlocksFetched,
    localBytesRead = taskEnd.taskMetrics.shuffleReadMetrics.localBytesRead,
    remoteBlocksFetched = taskEnd.taskMetrics.shuffleReadMetrics.remoteBlocksFetched,
    remoteBytesRead = taskEnd.taskMetrics.shuffleReadMetrics.remoteBytesRead,
    remoteBytesReadToDisk = taskEnd.taskMetrics.shuffleReadMetrics.remoteBytesReadToDisk,
    totalRecordsRead = taskEnd.taskMetrics.shuffleReadMetrics.recordsRead,
    remoteRequestsDuration = taskEnd.taskMetrics.shuffleReadMetrics.remoteReqsDuration,
    shuffleBytesWritten = taskEnd.taskMetrics.shuffleWriteMetrics.bytesWritten,
    shuffleRecordsWritten = taskEnd.taskMetrics.shuffleWriteMetrics.recordsWritten,
    shuffleWriteTime = taskEnd.taskMetrics.shuffleWriteMetrics.writeTime
  )
}
