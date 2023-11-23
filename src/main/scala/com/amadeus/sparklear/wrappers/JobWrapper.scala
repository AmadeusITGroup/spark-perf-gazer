package com.amadeus.sparklear.wrappers

import com.amadeus.sparklear.reports.JobReport
import com.amadeus.sparklear.wrappers.JobWrapper.EndUpdate
import org.apache.spark.scheduler.{SparkListenerJobEnd, SparkListenerJobStart, StageInfo}

import java.util.Properties

case class JobWrapper(
  name: String,
  group: String,
  sqlId: String,
  initialStages: Seq[StageRef],
  endUpdate: Option[EndUpdate] = None
) extends Wrapper[JobReport] {
  override def toReport(): JobReport = JobReport(this)
}

object JobWrapper {

  case class EndUpdate(
    finalStages: Seq[(StageRef, Option[StageWrapper])],
    jobEnd: SparkListenerJobEnd
  )

  // Copied from org.apache.spark.context to keep them in this package
  private val SPARK_JOB_DESCRIPTION = "spark.job.description"
  private val SPARK_JOB_GROUP_ID = "spark.jobGroup.id"
  private val SPARK_SQL_EXECUTION_ID = "spark.sql.execution.id"

  def from(jobStart: SparkListenerJobStart): JobWrapper = {
    from(jobStart.stageInfos, jobStart.properties)
  }
  def from(stageInfos: Seq[StageInfo], properties: Properties): JobWrapper = {
    val d = sanitize(properties.getProperty(SPARK_JOB_DESCRIPTION))
    val g = sanitize(properties.getProperty(SPARK_JOB_GROUP_ID))
    val i = properties.getProperty(SPARK_SQL_EXECUTION_ID)
    val s = stageInfos.map(i => StageRef(i.stageId, i.numTasks)).sortBy(_.id)
    JobWrapper(name = d, group = g, sqlId = i, initialStages = s)
  }

  private def sanitize(s: String): String = {
    s"$s".replace('\n', ' ')
  }
}
