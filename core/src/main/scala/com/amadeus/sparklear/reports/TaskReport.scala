package com.amadeus.sparklear.reports

import com.amadeus.sparklear.entities.TaskEntity

case class TaskReport(
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
) extends Report

object TaskReport extends Translator[TaskEntity, TaskReport] {
  def fromEntityToReport(r: TaskEntity): TaskReport = {
    val p = r.end
    TaskReport(
      stageId = p.stageId,
      taskId = p.taskId,
      taskDuration = p.taskDuration,
      taskLaunchTime = p.taskLaunchTime,
      taskFinishTime = p.taskFinishTime,
      executorRunTime = p.executorRunTime,
      executorCpuTime = p.executorCpuTime,
      executorDeserializeTime = p.executorDeserializeTime,
      executorDeserializeCpuTime = p.executorDeserializeCpuTime,
      resultSize = p.resultSize,
      diskBytesSpilled = p.diskBytesSpilled,
      memoryBytesSpilled = p.memoryBytesSpilled,
      bytesRead = p.bytesRead,
      recordsRead = p.recordsRead,
      jvmGCTime = p.jvmGCTime,
      bytesWritten = p.bytesWritten,
      recordsWritten = p.recordsWritten,
      peakExecutionMemory = p.peakExecutionMemory,
      resultSerializationTime = p.resultSerializationTime,
      fetchWaitTime = p.fetchWaitTime,
      localBlocksFetched = p.localBlocksFetched,
      localBytesRead = p.localBytesRead,
      remoteBlocksFetched = p.remoteBlocksFetched,
      remoteBytesRead = p.remoteBytesRead,
      remoteBytesReadToDisk = p.remoteBytesReadToDisk,
      totalRecordsRead = p.recordsRead,
      remoteRequestsDuration = p.remoteRequestsDuration,
      shuffleBytesWritten = p.bytesWritten,
      shuffleRecordsWritten = p.recordsWritten,
      shuffleWriteTime = p.shuffleWriteTime
    )
  }
}