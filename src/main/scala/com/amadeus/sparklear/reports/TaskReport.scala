package com.amadeus.sparklear.reports

import com.amadeus.sparklear.entities.TaskEntity
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{write => toJson}

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}

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
  override def asJson: Json = toJson(this)(DefaultFormats) // TODO: use a more efficient serialization
}

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

object TaskGenericRecord extends GenericTranslator[TaskReport, GenericRecord] {
  override val reportSchema: Schema = new Schema.Parser()
    .parse("""
             |{
             | "type": "record",
             | "name": "Root",
             | "fields": [
             |   {"name": "stageId", "type": "int"},
             |   {"name": "taskId", "type": "long"},
             |   {"name": "taskDuration", "type": "long"},
             |   {"name": "taskLaunchTime", "type": "long"},
             |   {"name": "taskFinishTime", "type": "long"},
             |   {"name": "executorRunTime", "type": "long"},
             |   {"name": "executorCpuTime", "type": "long"},
             |   {"name": "executorDeserializeTime", "type": "long"},
             |   {"name": "executorDeserializeCpuTime", "type": "long"},
             |   {"name": "resultSize", "type": "long"},
             |   {"name": "diskBytesSpilled", "type": "long"},
             |   {"name": "memoryBytesSpilled", "type": "long"},
             |   {"name": "bytesRead", "type": "long"},
             |   {"name": "recordsRead", "type": "long"},
             |   {"name": "jvmGCTime", "type": "long"},
             |   {"name": "bytesWritten", "type": "long"},
             |   {"name": "recordsWritten", "type": "long"},
             |   {"name": "peakExecutionMemory", "type": "long"},
             |   {"name": "resultSerializationTime", "type": "long"},
             |   {"name": "fetchWaitTime", "type": "long"},
             |   {"name": "localBlocksFetched", "type": "long"},
             |   {"name": "localBytesRead", "type": "long"},
             |   {"name": "remoteBlocksFetched", "type": "long"},
             |   {"name": "remoteBytesRead", "type": "long"},
             |   {"name": "remoteBytesReadToDisk", "type": "long"},
             |   {"name": "totalRecordsRead", "type": "long"},
             |   {"name": "remoteRequestsDuration", "type": "long"},
             |   {"name": "shuffleBytesWritten", "type": "long"},
             |   {"name": "shuffleRecordsWritten", "type": "long"},
             |   {"name": "shuffleWriteTime", "type": "long"}
             | ]
             |}""".stripMargin)

  override def fromReportToGenericRecord(r: TaskReport): GenericRecord = {
    val record = new GenericData.Record(reportSchema)
    record.put("stageId", r.stageId)
    record.put("taskId", r.taskId)
    record.put("taskDuration", r.taskDuration)
    record.put("taskLaunchTime", r.taskLaunchTime)
    record.put("taskFinishTime", r.taskFinishTime)
    record.put("executorRunTime", r.executorRunTime)
    record.put("executorCpuTime", r.executorCpuTime)
    record.put("executorDeserializeTime", r.executorDeserializeTime)
    record.put("executorDeserializeCpuTime", r.executorDeserializeCpuTime)
    record.put("resultSize", r.resultSize)
    record.put("diskBytesSpilled", r.diskBytesSpilled)
    record.put("memoryBytesSpilled", r.memoryBytesSpilled)
    record.put("bytesRead", r.bytesRead)
    record.put("recordsRead", r.recordsRead)
    record.put("jvmGCTime", r.jvmGCTime)
    record.put("bytesWritten", r.bytesWritten)
    record.put("recordsWritten", r.recordsWritten)
    record.put("peakExecutionMemory", r.peakExecutionMemory)
    record.put("resultSerializationTime", r.resultSerializationTime)
    record.put("fetchWaitTime", r.fetchWaitTime)
    record.put("localBlocksFetched", r.localBlocksFetched)
    record.put("localBytesRead", r.localBytesRead)
    record.put("remoteBlocksFetched", r.remoteBlocksFetched)
    record.put("remoteBytesRead", r.remoteBytesRead)
    record.put("remoteBytesReadToDisk", r.remoteBytesReadToDisk)
    record.put("totalRecordsRead", r.recordsRead)
    record.put("remoteRequestsDuration", r.remoteRequestsDuration)
    record.put("shuffleBytesWritten", r.bytesWritten)
    record.put("shuffleRecordsWritten", r.recordsWritten)
    record.put("shuffleWriteTime", r.shuffleWriteTime)
    record
  }
}
