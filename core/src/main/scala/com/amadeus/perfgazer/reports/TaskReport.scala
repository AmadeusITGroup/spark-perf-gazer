package com.amadeus.perfgazer.reports

import com.amadeus.perfgazer.events.TaskEvent

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
) extends Report {
  override def reportType: ReportType = TaskReportType
}

object TaskReport {

  /** Create a TaskReport
    *
    * @param end the TaskEvent for task end
    * @return the TaskReport generated
    */
  def apply(end: TaskEvent): TaskReport = {
    TaskReport(
      stageId = end.stageId,
      taskId = end.taskId,
      taskDuration = end.taskDuration,
      taskLaunchTime = end.taskLaunchTime,
      taskFinishTime = end.taskFinishTime,
      executorRunTime = end.executorRunTime,
      executorCpuTime = end.executorCpuTime,
      executorDeserializeTime = end.executorDeserializeTime,
      executorDeserializeCpuTime = end.executorDeserializeCpuTime,
      resultSize = end.resultSize,
      diskBytesSpilled = end.diskBytesSpilled,
      memoryBytesSpilled = end.memoryBytesSpilled,
      bytesRead = end.bytesRead,
      recordsRead = end.recordsRead,
      jvmGCTime = end.jvmGCTime,
      bytesWritten = end.bytesWritten,
      recordsWritten = end.recordsWritten,
      peakExecutionMemory = end.peakExecutionMemory,
      resultSerializationTime = end.resultSerializationTime,
      fetchWaitTime = end.fetchWaitTime,
      localBlocksFetched = end.localBlocksFetched,
      localBytesRead = end.localBytesRead,
      remoteBlocksFetched = end.remoteBlocksFetched,
      remoteBytesRead = end.remoteBytesRead,
      remoteBytesReadToDisk = end.remoteBytesReadToDisk,
      totalRecordsRead = end.recordsRead,
      remoteRequestsDuration = end.remoteRequestsDuration,
      shuffleBytesWritten = end.bytesWritten,
      shuffleRecordsWritten = end.recordsWritten,
      shuffleWriteTime = end.shuffleWriteTime
    )
  }

}
