package com.amadeus.sparklear.reports

import com.amadeus.sparklear.entities.StageEntity

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}

case class StageReport(
  stageId: Int,
  readBytes: Long,
  writeBytes: Long,
  shuffleReadBytes: Long,
  shuffleWriteBytes: Long,
  execCpuNs: Long,
  execRunNs: Long,
  execJvmGcNs: Long,
  attempt: Int,
  spillBytes: Long
) extends Report

object StageReport extends Translator[StageEntity, StageReport] {

  def fromEntityToReport(r: StageEntity): StageReport = {
    val p = r.end
    StageReport(
      stageId = r.end.stageInfo.stageId,
      readBytes = p.inputReadBytes,
      writeBytes = p.outputWriteBytes,
      shuffleReadBytes = p.shuffleReadBytes,
      shuffleWriteBytes = p.shuffleWriteBytes,
      execCpuNs = p.execCpuNs,
      execRunNs = p.execRunNs,
      execJvmGcNs = p.execjvmGCNs,
      attempt = p.attempt,
      spillBytes = p.spillBytes
    )
  }
}

object StageGenericRecord extends GenericTranslator[StageReport, GenericRecord] {
  override val reportSchema: Schema = new Schema.Parser()
    .parse("""
             |{
             | "type": "record",
             | "name": "Root",
             | "fields": [
             |   {"name": "stageId", "type": "int"},
             |   {"name": "readBytes", "type": "long"},
             |   {"name": "writeBytes", "type": "long"},
             |   {"name": "shuffleReadBytes", "type": "long"},
             |   {"name": "shuffleWriteBytes", "type": "long"},
             |   {"name": "execCpuNs", "type": "long"},
             |   {"name": "execRunNs", "type": "long"},
             |   {"name": "execJvmGcNs", "type": "long"},
             |   {"name": "attempt", "type": "int"},
             |   {"name": "spillBytes", "type": "long"}
             | ]
             |}""".stripMargin)

  override def fromReportToGenericRecord(r: StageReport): GenericRecord = {
    val record = new GenericData.Record(reportSchema)
    record.put("stageId", r.stageId)
    record.put("readBytes", r.readBytes)
    record.put("writeBytes", r.writeBytes)
    record.put("shuffleReadBytes", r.shuffleReadBytes)
    record.put("shuffleWriteBytes", r.shuffleWriteBytes)
    record.put("execCpuNs", r.execCpuNs)
    record.put("execRunNs", r.execRunNs)
    record.put("execJvmGcNs", r.execJvmGcNs)
    record.put("attempt", r.attempt)
    record.put("spillBytes", r.spillBytes)
    record
  }
}
