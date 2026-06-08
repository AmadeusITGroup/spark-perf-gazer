package com.amadeus.perfgazer.reports

import com.amadeus.perfgazer.events.TaskEvent
import com.amadeus.perfgazer.schema.{ColumnDoc, TableDoc}

@TableDoc(name = "task", description = "Task-level execution metrics. One row per completed Spark task.")
case class TaskReport(
  @ColumnDoc(description = "Stage this task belongs to")
  stageId: Int,
  @ColumnDoc(description = "Unique task identifier")
  taskId: Long,
  @ColumnDoc(description = "Wall-clock duration of the task", unit = "ms")
  taskDuration: Long,
  @ColumnDoc(description = "Epoch timestamp when the task was launched", unit = "ms")
  taskLaunchTime: Long,
  @ColumnDoc(description = "Epoch timestamp when the task finished", unit = "ms")
  taskFinishTime: Long,
  @ColumnDoc(description = "Time spent running the task on the executor", unit = "ms")
  executorRunTime: Long,
  @ColumnDoc(description = "CPU time consumed by the executor", unit = "ns")
  executorCpuTime: Long,
  @ColumnDoc(description = "Time to deserialize the task on the executor", unit = "ms")
  executorDeserializeTime: Long,
  @ColumnDoc(description = "CPU time spent deserializing the task", unit = "ns")
  executorDeserializeCpuTime: Long,
  @ColumnDoc(description = "Size of the serialized task result", unit = "bytes")
  resultSize: Long,
  @ColumnDoc(description = "Bytes spilled to disk", unit = "bytes")
  diskBytesSpilled: Long,
  @ColumnDoc(description = "Bytes spilled to memory", unit = "bytes")
  memoryBytesSpilled: Long,
  @ColumnDoc(description = "Input bytes read", unit = "bytes")
  bytesRead: Long,
  @ColumnDoc(description = "Input records read")
  recordsRead: Long,
  @ColumnDoc(description = "Time spent in JVM garbage collection", unit = "ms")
  jvmGCTime: Long,
  @ColumnDoc(description = "Output bytes written", unit = "bytes")
  bytesWritten: Long,
  @ColumnDoc(description = "Output records written")
  recordsWritten: Long,
  @ColumnDoc(description = "Peak execution memory used", unit = "bytes")
  peakExecutionMemory: Long,
  @ColumnDoc(description = "Time spent serializing the result", unit = "ms")
  resultSerializationTime: Long,
  @ColumnDoc(description = "Time spent waiting for shuffle fetch", unit = "ms")
  fetchWaitTime: Long,
  @ColumnDoc(description = "Number of local blocks fetched during shuffle")
  localBlocksFetched: Long,
  @ColumnDoc(description = "Bytes read from local shuffle blocks", unit = "bytes")
  localBytesRead: Long,
  @ColumnDoc(description = "Number of remote blocks fetched during shuffle")
  remoteBlocksFetched: Long,
  @ColumnDoc(description = "Bytes read from remote shuffle blocks", unit = "bytes")
  remoteBytesRead: Long,
  @ColumnDoc(description = "Remote shuffle bytes read to disk", unit = "bytes")
  remoteBytesReadToDisk: Long,
  @ColumnDoc(description = "Total records read including shuffle")
  totalRecordsRead: Long,
  @ColumnDoc(description = "Time spent on remote shuffle requests", unit = "ms")
  remoteRequestsDuration: Long,
  @ColumnDoc(description = "Shuffle bytes written", unit = "bytes")
  shuffleBytesWritten: Long,
  @ColumnDoc(description = "Shuffle records written")
  shuffleRecordsWritten: Long,
  @ColumnDoc(description = "Time spent writing shuffle data", unit = "ns")
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
      totalRecordsRead = end.totalRecordsRead,
      remoteRequestsDuration = end.remoteRequestsDuration,
      shuffleBytesWritten = end.shuffleBytesWritten,
      shuffleRecordsWritten = end.shuffleRecordsWritten,
      shuffleWriteTime = end.shuffleWriteTime
    )
  }

}
