package com.amadeus.sparklear.reports
import com.amadeus.sparklear.Config
import com.amadeus.sparklear.entities.JobEntity
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{write => toJson}

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
