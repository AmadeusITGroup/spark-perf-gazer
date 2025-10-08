package com.amadeus.sparklear.reports

import com.amadeus.sparklear.entities.JobEntity

case class JobReport(
  jobId: Long,
  groupId: String,
  jobName: String,
  jobStartTime: Long,
  jobEndTime: Long,
  sqlId: String,
  stages: Seq[Int]
) extends Report

object JobReport extends Translator[JobEntity, JobReport] {
  override def fromEntityToReport(e: JobEntity): JobReport = {
    val startEvt = e.start
    val endEvt = e.end
    JobReport(
      jobId = endEvt.jobEnd.jobId,
      jobStartTime = startEvt.startTime,
      jobEndTime = endEvt.jobEnd.time,
      groupId = startEvt.group,
      jobName = startEvt.name,
      sqlId = startEvt.sqlId,
      stages = startEvt.initialStages.map(_.id)
    )
  }
}