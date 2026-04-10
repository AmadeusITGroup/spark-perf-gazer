package com.amadeus.perfgazer.reports

import com.amadeus.perfgazer.events.TaskEvent
import com.amadeus.perfgazer.schema.{SchemaDoc, SchemaReport}

@SchemaReport("task", "Task-level execution metrics. One row per completed Spark task.")
case class TaskReport(
  @SchemaDoc("Stage this task belongs to")
  stageId: Int,
  @SchemaDoc("Unique task identifier")
  taskId: Long,
  @SchemaDoc("Wall-clock duration of the task", unit = "ms")
  taskDuration: Long,
  @SchemaDoc("Epoch timestamp when the task was launched", unit = "ms")
  taskLaunchTime: Long,
  @SchemaDoc("Epoch timestamp when the task finished", unit = "ms")
  taskFinishTime: Long,
  @SchemaDoc("Time spent running the task on the executor", unit = "ms")
  executorRunTime: Long,
  @SchemaDoc("CPU time consumed by the executor", unit = "ns")
  executorCpuTime: Long,
  @SchemaDoc("Time to deserialize the task on the executor", unit = "ms")
  executorDeserializeTime: Long,
  @SchemaDoc("CPU time spent deserializing the task", unit = "ns")
  executorDeserializeCpuTime: Long,
  @SchemaDoc("Size of the serialized task result", unit = "bytes")
  resultSize: Long,
  @SchemaDoc("Bytes spilled to disk", unit = "bytes")
  diskBytesSpilled: Long,
  @SchemaDoc("Bytes spilled to memory", unit = "bytes")
  memoryBytesSpilled: Long,
  @SchemaDoc("Input bytes read", unit = "bytes")
  bytesRead: Long,
  @SchemaDoc("Input records read")
  recordsRead: Long,
  @SchemaDoc("Time spent in JVM garbage collection", unit = "ms")
  jvmGCTime: Long,
  @SchemaDoc("Output bytes written", unit = "bytes")
  bytesWritten: Long,
  @SchemaDoc("Output records written")
  recordsWritten: Long,
  @SchemaDoc("Peak execution memory used", unit = "bytes")
  peakExecutionMemory: Long,
  @SchemaDoc("Time spent serializing the result", unit = "ms")
  resultSerializationTime: Long,
  @SchemaDoc("Time spent waiting for shuffle fetch", unit = "ms")
  fetchWaitTime: Long,
  @SchemaDoc("Number of local blocks fetched during shuffle")
  localBlocksFetched: Long,
  @SchemaDoc("Bytes read from local shuffle blocks", unit = "bytes")
  localBytesRead: Long,
  @SchemaDoc("Number of remote blocks fetched during shuffle")
  remoteBlocksFetched: Long,
  @SchemaDoc("Bytes read from remote shuffle blocks", unit = "bytes")
  remoteBytesRead: Long,
  @SchemaDoc("Remote shuffle bytes read to disk", unit = "bytes")
  remoteBytesReadToDisk: Long,
  @SchemaDoc("Total records read including shuffle")
  totalRecordsRead: Long,
  @SchemaDoc("Time spent on remote shuffle requests", unit = "ms")
  remoteRequestsDuration: Long,
  @SchemaDoc("Shuffle bytes written", unit = "bytes")
  shuffleBytesWritten: Long,
  @SchemaDoc("Shuffle records written")
  shuffleRecordsWritten: Long,
  @SchemaDoc("Time spent writing shuffle data", unit = "ns")
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
