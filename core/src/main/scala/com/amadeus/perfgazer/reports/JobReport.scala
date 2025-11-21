package com.amadeus.perfgazer.reports

import com.amadeus.perfgazer.events.JobEvent

case class JobReport(
  jobId: Long,
  groupId: String,
  jobName: String,
  jobStartTime: Long,
  jobEndTime: Long,
  sqlId: String,
  stages: Seq[Int]
) extends Report

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
