package com.amadeus.sparklear.reports
import com.amadeus.sparklear.Config
import com.amadeus.sparklear.entities.JobEntity
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{write => toJson}

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}

case class JobReport(
  jobId: Long,
  groupId: String,
  jobName: String,
  sqlId: String,
  stages: Int
) extends Report {
  override def asJson: Json = toJson(this)(DefaultFormats) // TODO: use a more efficient serialization
}

object JobReport extends Translator[JobEntity, JobReport] {
  override def fromEntityToReport(e: JobEntity): JobReport = {
    val col = e.start
    val end = e.end
    JobReport(
      jobId = end.jobEnd.jobId,
      groupId = col.group,
      jobName = col.name,
      sqlId = col.sqlId,
      stages = col.initialStages.size
    )
  }
}

object JobGenericRecord extends GenericTranslator[JobReport, GenericRecord] {
  override val reportSchema: Schema = new Schema.Parser()
    .parse("""
             |{
             | "type": "record",
             | "name": "Root",
             | "fields": [
             |   {"name": "jobId", "type": "long"},
             |   {"name": "groupId", "type": "string"},
             |   {"name": "jobName", "type": "string"},
             |   {"name": "sqlId", "type": ["null", "string"]},
             |   {"name": "stages", "type": "int"}
             | ]
             |}""".stripMargin)

  override def fromReportToGenericRecord(r: JobReport): GenericRecord = {
    val record = new GenericData.Record(reportSchema)
    record.put("jobId", r.jobId)
    record.put("groupId", r.groupId)
    record.put("jobName", r.jobName)
    record.put("sqlId", r.sqlId)
    record.put("stages", r.stages)
    record
  }
}