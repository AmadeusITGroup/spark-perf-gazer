package com.amadeus.perfgazer.reports

import com.amadeus.perfgazer.events.JobEvent
import com.amadeus.perfgazer.schema.{SchemaDoc, SchemaReport}

@SchemaReport(value = "job", description = "Job-level execution report. One row per completed Spark job.")
case class JobReport(
  @SchemaDoc(value = "Unique job identifier")
  jobId: Long,
  @SchemaDoc(value = "Job group identifier")
  groupId: String,
  @SchemaDoc(value = "Name of the job")
  jobName: String,
  @SchemaDoc(value = "Epoch timestamp when the job started", unit = "ms")
  jobStartTime: Long,
  @SchemaDoc(value = "Epoch timestamp when the job ended", unit = "ms")
  jobEndTime: Long,
  @SchemaDoc(value = "Associated SQL execution identifier")
  sqlId: String,
  @SchemaDoc(value = "List of stage IDs in this job")
  stages: Seq[Int]
) extends Report {
  override def reportType: ReportType = JobReportType
}

object JobReport{

  /** Create a JobReport
    *
    * @param start the JobEvent for job start
    * @param end the EndUpdate for job end
    * @return the JobReport generated
    */
  def apply(start: JobEvent, end: JobEvent.EndUpdate): JobReport = {
    JobReport(
      jobId = end.jobEnd.jobId,
      jobStartTime = start.startTime,
      jobEndTime = end.jobEnd.time,
      groupId = start.group,
      jobName = start.name,
      sqlId = start.sqlId,
      stages = start.initialStages.map(_.id)
    )
  }
}
