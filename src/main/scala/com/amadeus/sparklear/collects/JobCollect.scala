package com.amadeus.sparklear.collects

import com.amadeus.sparklear.input.JobInput
import com.amadeus.sparklear.collects.JobCollect.EndUpdate
import org.apache.spark.scheduler.{SparkListenerJobEnd, SparkListenerJobStart, StageInfo}

import java.util.Properties

case class JobCollect(
  name: String,
  group: String,
  sqlId: String,
  initialStages: Seq[StageRef]
) extends Collect[JobInput] {
}

object JobCollect {

  case class EndUpdate(
                        finalStages: Seq[(StageRef, Option[StageCollect])],
                        jobEnd: SparkListenerJobEnd
  )

  // Copied from org.apache.spark.context to keep them in this package
  private val SPARK_JOB_DESCRIPTION = "spark.job.description"
  private val SPARK_JOB_GROUP_ID = "spark.jobGroup.id"
  private val SPARK_SQL_EXECUTION_ID = "spark.sql.execution.id"

  def from(jobStart: SparkListenerJobStart): JobCollect = {
    from(jobStart.stageInfos, jobStart.properties)
  }
  def from(stageInfos: Seq[StageInfo], properties: Properties): JobCollect = {
    val d = sanitize(properties.getProperty(SPARK_JOB_DESCRIPTION))
    val g = sanitize(properties.getProperty(SPARK_JOB_GROUP_ID))
    val i = properties.getProperty(SPARK_SQL_EXECUTION_ID)
    val s = stageInfos.map(i => StageRef(i.stageId, i.numTasks)).sortBy(_.id)
    JobCollect(name = d, group = g, sqlId = i, initialStages = s)
  }

  private def sanitize(s: String): String = {
    s"$s".replace('\n', ' ')
  }
}
