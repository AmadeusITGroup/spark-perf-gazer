package com.amadeus.sparklear.events

import com.amadeus.sparklear.entities.TaskEntity
import org.apache.spark.scheduler.SparkListenerTaskEnd

/** Raw event proving information about a Task
  */
case class TaskEvent(taskEnd: SparkListenerTaskEnd) extends Event[TaskEntity] {
  val stageId: Int = taskEnd.stageId
  val taskId: Long = taskEnd.taskInfo.taskId
  val taskDuration: Long = taskEnd.taskInfo.duration
  val taskLaunchTime: Long = taskEnd.taskInfo.launchTime
  val taskFinishTime: Long = taskEnd.taskInfo.finishTime
  val executorRunTime: Long = taskEnd.taskMetrics.executorRunTime
  val executorCpuTime: Long = taskEnd.taskMetrics.executorCpuTime
  val executorDeserializeTime: Long = taskEnd.taskMetrics.executorDeserializeTime
  val executorDeserializeCpuTime: Long = taskEnd.taskMetrics.executorDeserializeCpuTime
  val resultSize: Long = taskEnd.taskMetrics.resultSize
  val diskBytesSpilled: Long = taskEnd.taskMetrics.diskBytesSpilled
  val memoryBytesSpilled: Long = taskEnd.taskMetrics.memoryBytesSpilled
  val bytesRead: Long = taskEnd.taskMetrics.inputMetrics.bytesRead
  val recordsRead: Long = taskEnd.taskMetrics.inputMetrics.recordsRead
  val jvmGCTime: Long = taskEnd.taskMetrics.jvmGCTime
  val bytesWritten: Long = taskEnd.taskMetrics.outputMetrics.bytesWritten
  val recordsWritten: Long = taskEnd.taskMetrics.outputMetrics.recordsWritten
  val peakExecutionMemory: Long = taskEnd.taskMetrics.peakExecutionMemory
  val resultSerializationTime: Long = taskEnd.taskMetrics.resultSerializationTime
  val fetchWaitTime: Long = taskEnd.taskMetrics.shuffleReadMetrics.fetchWaitTime
  val localBlocksFetched: Long = taskEnd.taskMetrics.shuffleReadMetrics.localBlocksFetched
  val localBytesRead: Long = taskEnd.taskMetrics.shuffleReadMetrics.localBytesRead
  val remoteBlocksFetched: Long = taskEnd.taskMetrics.shuffleReadMetrics.remoteBlocksFetched
  val remoteBytesRead: Long = taskEnd.taskMetrics.shuffleReadMetrics.remoteBytesRead
  val remoteBytesReadToDisk: Long = taskEnd.taskMetrics.shuffleReadMetrics.remoteBytesReadToDisk
  val totalRecordsRead: Long = taskEnd.taskMetrics.shuffleReadMetrics.recordsRead
  val remoteRequestsDuration: Long = taskEnd.taskMetrics.shuffleReadMetrics.remoteReqsDuration
  val shuffleBytesWritten: Long = taskEnd.taskMetrics.shuffleWriteMetrics.bytesWritten
  val shuffleRecordsWritten: Long = taskEnd.taskMetrics.shuffleWriteMetrics.recordsWritten
  val shuffleWriteTime: Long = taskEnd.taskMetrics.shuffleWriteMetrics.writeTime
}