package com.amadeus.sparklear.events

import com.amadeus.sparklear.entities.JobEntity
import org.apache.spark.scheduler.{SparkListenerJobEnd, SparkListenerJobStart, StageInfo}

import java.util.Properties

/** Raw event proving information about a Job
  */
case class JobEvent(
  name: String,
  group: String,
  sqlId: String,
  initialStages: Seq[StageRef]
) extends Event[JobEntity] {}

object JobEvent {

  case class EndUpdate(
    finalStages: Seq[(StageRef, Option[StageEvent])], // TODO can be removed, keep stage stuff at stage level
    jobEnd: SparkListenerJobEnd
  )

  // Copied from org.apache.spark.context to keep them in this package
  private val SPARK_JOB_DESCRIPTION = "spark.job.description"
  private val SPARK_JOB_GROUP_ID = "spark.jobGroup.id"
  private val SPARK_SQL_EXECUTION_ID = "spark.sql.execution.id"

  def from(jobStart: SparkListenerJobStart): JobEvent = {
    from(jobStart.stageInfos, jobStart.properties)
  }
  private def from(stageInfos: Seq[StageInfo], properties: Properties): JobEvent = {
    val d = sanitize(properties.getProperty(SPARK_JOB_DESCRIPTION))
    val g = sanitize(properties.getProperty(SPARK_JOB_GROUP_ID))
    val i = properties.getProperty(SPARK_SQL_EXECUTION_ID)
    val s = stageInfos.map(i => StageRef(i.stageId, i.numTasks)).sortBy(_.id)
    JobEvent(name = d, group = g, sqlId = i, initialStages = s)
  }

  private def sanitize(s: String): String = {
    s"$s".replace('\n', ' ')
  }
}
