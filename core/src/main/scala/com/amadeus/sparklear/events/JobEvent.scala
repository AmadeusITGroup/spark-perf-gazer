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
  id: Long,
  startTime: Long,
  initialStages: Seq[StageRef]
) extends Event[JobEntity] {}

object JobEvent {

  case class EndUpdate(
    jobEnd: SparkListenerJobEnd
  )

  // Copied from org.apache.spark.context to keep them in this package
  private val SPARK_JOB_DESCRIPTION = "spark.job.description"
  private val SPARK_JOB_GROUP_ID = "spark.jobGroup.id"
  private val SPARK_SQL_EXECUTION_ID = "spark.sql.execution.id"

  def from(jobStart: SparkListenerJobStart): JobEvent = {
    val properties = jobStart.properties
    val stageInfos = jobStart.stageInfos
    val d = sanitize(properties.getProperty(SPARK_JOB_DESCRIPTION))
    val g = sanitize(properties.getProperty(SPARK_JOB_GROUP_ID))
    val i = properties.getProperty(SPARK_SQL_EXECUTION_ID)
    val s = stageInfos.map(i => StageRef(i.stageId, i.numTasks)).sortBy(_.id)
    val jobId = jobStart.jobId
    val jobStartTime = jobStart.time
    JobEvent(id = jobId, startTime = jobStartTime,  name = d, group = g, sqlId = i, initialStages = s)
  }

  private def sanitize(s: String): String = {
    s"$s".replace('\n', ' ')
  }
}
