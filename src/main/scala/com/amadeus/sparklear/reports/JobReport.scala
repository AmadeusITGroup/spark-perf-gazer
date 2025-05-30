package com.amadeus.sparklear.reports
import com.amadeus.sparklear.Config
import com.amadeus.sparklear.entities.JobEntity
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{write => toJson}

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
