package com.amadeus.sparklear.reports

import com.amadeus.sparklear.entities.StageEntity
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{write => toJson}

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
) extends Report {
  override def asJson: Json = toJson(this)(DefaultFormats) // TODO: use a more efficient serialization
}

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
             |   {"name": "readMb", "type": "long"},
             |   {"name": "writeMb", "type": "long"},
             |   {"name": "shuffleReadMb", "type": "long"},
             |   {"name": "shuffleWriteMb", "type": "long"},
             |   {"name": "execCpuSecs", "type": "long"},
             |   {"name": "execRunSecs", "type": "long"},
             |   {"name": "execJvmGcSecs", "type": "long"},
             |   {"name": "attempt", "type": "int"},
             |   {"name": "spillMb", "type": "long"}
             | ]
             |}""".stripMargin)

  override def fromReportToGenericRecord(r: StageReport): GenericRecord = {
    val record = new GenericData.Record(reportSchema)
    record.put("stageId", r.stageId)
    record.put("readMb", r.readMb)
    record.put("writeMb", r.writeMb)
    record.put("shuffleReadMb", r.shuffleReadMb)
    record.put("shuffleWriteMb", r.shuffleWriteMb)
    record.put("execCpuSecs", r.execCpuSecs)
    record.put("execRunSecs", r.execRunSecs)
    record.put("execJvmGcSecs", r.execJvmGcSecs)
    record.put("attempt", r.attempt)
    record.put("spillMb", r.spillMb)
    record
  }
}
