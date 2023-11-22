package com.amadeus.sparklear.wrappers

import com.amadeus.sparklear.reports.JobReport
import org.apache.spark.scheduler.StageInfo

import java.util.Properties

case class JobWrapper(
  name: String,
  group: String,
  sqlId: String,
  stages: Seq[StageRef]
) extends Wrapper[JobReport] {
  override def toReport(): JobReport = JobReport(this)
}

object JobWrapper {

  // Copied from org.apache.spark.context to keep them in this package
  private val SPARK_JOB_DESCRIPTION = "spark.job.description"
  private val SPARK_JOB_GROUP_ID = "spark.jobGroup.id"
  private val SPARK_SQL_EXECUTION_ID = "spark.sql.execution.id"

  def from(stageInfos: Seq[StageInfo], properties: Properties): JobWrapper = {
    val d = sanitize(properties.getProperty(SPARK_JOB_DESCRIPTION))
    val g = sanitize(properties.getProperty(SPARK_JOB_GROUP_ID))
    val i = properties.getProperty(SPARK_SQL_EXECUTION_ID)
    val s = stageInfos.map(i => StageRef(i.stageId, i.numTasks)).sortBy(_.id)
    JobWrapper(d, g, i, s)
  }

  private def sanitize(s: String): String = {
    s"$s".replace('\n', ' ')
  }
}
