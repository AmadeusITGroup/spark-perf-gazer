package com.amadeus.sparklear.reports
import com.amadeus.sparklear.Config
import com.amadeus.sparklear.entities.JobEntity
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{write => toJson}

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import scala.collection.JavaConverters._

case class JobReport(
  jobId: Long,
  groupId: String,
  jobName: String,
  jobStartTime: Long,
  jobDuration: Long,
  sqlId: String,
  stages: Seq[Int]
) extends Report {
  override def asJson: Json = toJson(this)(DefaultFormats)
}

object JobReport extends Translator[JobEntity, JobReport] {
  override def fromEntityToReport(e: JobEntity): JobReport = {
    val startEvt = e.start
    val endEvt = e.end
    JobReport(
      jobId = endEvt.jobEnd.jobId,
      jobStartTime = startEvt.startTime,
      jobDuration = endEvt.jobEnd.time,
      groupId = startEvt.group,
      jobName = startEvt.name,
      sqlId = startEvt.sqlId,
      stages = startEvt.initialStages.map(_.id)
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
             |   {"name": "jobStartTime", "type": "long"},
             |   {"name": "jobDuration", "type": "long"},
             |   {"name": "sqlId", "type": "string"},
             |   { "name": "stages", "type": { "type": "array", "items": "int" } }
             | ]
             |}""".stripMargin)

  override def fromReportToGenericRecord(r: JobReport): GenericRecord = {
    val record = new GenericData.Record(reportSchema)
    record.put("jobId", r.jobId)
    record.put("groupId", r.groupId)
    record.put("jobName", r.jobName)
    record.put("jobStartTime", r.jobStartTime)
    record.put("jobDuration", r.jobDuration)
    record.put("sqlId", r.sqlId)
    record.put("stages", r.stages.asJava)
    record
  }
}