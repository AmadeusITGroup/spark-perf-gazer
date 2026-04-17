package com.amadeus.perfgazer.reports

import com.amadeus.perfgazer.events.JobEvent
import com.amadeus.perfgazer.schema.{ColumnDoc, TableDoc}

@TableDoc(name = "job", description = "Job-level execution report. One row per completed Spark job.")
case class JobReport(
  @ColumnDoc(description = "Unique job identifier")
  jobId: Long,
  @ColumnDoc(description = "Job group identifier")
  groupId: String,
  @ColumnDoc(description = "Name of the job")
  jobName: String,
  @ColumnDoc(description = "Epoch timestamp when the job started", unit = "ms")
  jobStartTime: Long,
  @ColumnDoc(description = "Epoch timestamp when the job ended", unit = "ms")
  jobEndTime: Long,
  @ColumnDoc(description = "Associated SQL execution identifier")
  sqlId: String,
  @ColumnDoc(description = "List of stage IDs in this job")
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
