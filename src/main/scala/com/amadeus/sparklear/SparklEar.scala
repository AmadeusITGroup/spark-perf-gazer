package com.amadeus.sparklear

import com.amadeus.sparklear.entities.{Entity, JobEntity, SqlEntity, StageEntity}
import com.amadeus.sparklear.utils.CappedConcurrentHashMap
import com.amadeus.sparklear.events.JobEvent.EndUpdate
import com.amadeus.sparklear.events.{JobEvent, SqlEvent, StageEvent}
import com.amadeus.sparklear.reports.{JobReport, SqlReport, StageReport}
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.ui._
import org.slf4j.{Logger, LoggerFactory}

/** This listener displays in the Spark Driver STDOUT some
  * relevant information about the application, including:
  * - total executor CPU time
  * - spilled tasks
  * - ...
  */
class SparklEar(c: Config) extends SparkListener {

  implicit lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  type SqlKey = Long
  type JobKey = Int
  type StageKey = Int

  // Maps to keep sqls + jobs + stages raw events (initial information) until some completion
  private val sqlRawEvents = new CappedConcurrentHashMap[SqlKey, SqlEvent](c.maxCacheSize)
  private val jobRawEvents = new CappedConcurrentHashMap[JobKey, JobEvent](c.maxCacheSize)
  private val stageRawEvents = new CappedConcurrentHashMap[StageKey, StageEvent](c.maxCacheSize)

  /** LISTENERS
    */

  /** This is the listener method for stage end
    *
    * It is NOT a trigger for automatic purge of stages (job end will purge stages).
    */
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    logger.trace("onStageCompleted(...)")
    // generate a stage event
    val sw = StageEvent(stageCompleted.stageInfo)
    // store stage collect for the use in jobs
    stageRawEvents.put(stageCompleted.stageInfo.stageId, sw)

    if (c.stagesEnabled) {
      logger.trace("Handling Stage end: {}", stageCompleted.stageInfo.stageId)
      // generate the stage input
      val si = StageEntity(sw)
      // sink the stage input serialized (as string, and as objects)
      c.sink.sink(StageReport.fromEntityToReport(si))
    } else {
      logger.trace("Ignoring Stage end: {}", stageCompleted.stageInfo.stageId)
    }

    // nothing to purge
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    logger.trace("onJobStart(...)")
    jobRawEvents.put(jobStart.jobId, JobEvent.from(jobStart))
  }

  /** This is the listener method for job end
    *
    * It is a trigger for automatic purge of job and stages.
    */
  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    logger.trace("onJobEnd(...)")
    val jobId = jobEnd.jobId
    val jobCollectOpt = Option(jobRawEvents.get(jobId)) // retrieve initial image of job (it could have been purged)
    jobCollectOpt.foreach { jobCollect =>
      val stagesIdAndStats = jobCollect.initialStages.map { sd => // retrieve image of stages
        (sd, Option(stageRawEvents.get(sd.id)))
      }

      if (c.jobsEnabled) {
        // generate the job input
        val ji = JobEntity(jobCollect, EndUpdate(finalStages = stagesIdAndStats, jobEnd = jobEnd))
        // sink the job input serialized (as string, and as objects)
        logger.trace("Handling Job end: {}", jobEnd.jobId)
        c.sink.sink(JobReport.fromEntityToReport(ji))
      } else {
        logger.trace("Ignoring Job end: {}", jobEnd.jobId)
      }

      // purge
      jobRawEvents.remove(jobId)
      val stageIds = jobCollect.initialStages.map(_.id)
      stageIds.foreach(i => stageRawEvents.remove(i))
    }
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    logger.trace("onOtherEvent(...)")
    event match {
      case event: SparkListenerSQLExecutionStart =>
        onSqlStart(event)
      //case event: SparkListenerDriverAccumUpdates =>
      case event: SparkListenerSQLExecutionEnd =>
        onSqlEnd(event)
      case e =>
        logger.trace("Event ignored: {}", e.getClass.getName)
      //case _: SparkListenerSQLAdaptiveSQLMetricUpdates =>
      // TODO: ignored for now, maybe adds more metrics?
      //case _: SparkListenerSQLAdaptiveExecutionUpdate =>
      // TODO: ignored for now, maybe adds more metrics?
    }
  }

  private def onSqlStart(event: SparkListenerSQLExecutionStart): Unit = {
    logger.trace("onSqlStart(...)")
    sqlRawEvents.put(event.executionId, SqlEvent(event.executionId, event.sparkPlanInfo, event.description))
  }

  /** This is the listener method for SQL query end
    *
    * It is a trigger for automatic purge of SQL queries and involved metrics.
    */
  private def onSqlEnd(event: SparkListenerSQLExecutionEnd): Unit = {
    logger.trace("onSqlEnd(...)")

    // get the initial sql collect information (it could have been purged)
    val sqlCollectOpt = Option(sqlRawEvents.get(event.executionId))

    sqlCollectOpt.foreach { sqlCollect =>
      if (c.sqlEnabled) {
        logger.trace("Handling SQL end: {}", event.executionId)
        // generate the SQL input
        val si = SqlEntity(sqlCollect, event)
        // sink the SQL input serialized (as string, and as objects)
        c.sink.sink(SqlReport.fromEntityToReport(si))
      } else {
        logger.trace("Ignoring SQL end: {}", event.executionId)
      }

      // purge
      sqlRawEvents.remove(event.executionId)
    }
  }

  override def onApplicationEnd(event: SparkListenerApplicationEnd): Unit = {
    logger.trace("onApplicationEnd: duration={}", event.time)
  }

}
