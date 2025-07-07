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

  // Maps to keep sqls + jobs + stages raw events (initial information) until some completion
  private val sqlStartEvents = new CappedConcurrentHashMap[SqlKey, SqlEvent]("sql", c.maxCacheSize)
  private val jobStartEvents = new CappedConcurrentHashMap[JobKey, JobEvent]("job", c.maxCacheSize)

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
    if (c.stagesEnabled) {
      logger.trace("Handling Stage end: {}", stageCompleted.stageInfo.stageId)
      // generate the stage input
      val si = StageEntity(sw)
      // sink the stage input serialized (as string, and as objects)
      c.sink.sink(Seq(StageReport.fromEntityToReport(si) : StageReport))
    } else {
      logger.trace("Ignoring Stage end: {}", stageCompleted.stageInfo.stageId)
    }
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    logger.trace("onJobStart(...)")
    jobStartEvents.put(jobStart.jobId, JobEvent.from(jobStart))
  }

  /** This is the listener method for job end
    *
    * It is a trigger for automatic purge of job and stages.
    */
  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    logger.trace("onJobEnd(...)")
    val jobId = jobEnd.jobId
    val jobStartOpt = Option(jobStartEvents.get(jobId)) // retrieve initial image of job
    var jobReports: Seq[JobReport] = Seq()
    jobStartOpt.foreach { jobStart =>
      if (c.jobsEnabled) {
        logger.trace("Handling Job end: {}", jobEnd.jobId)
        val ji = JobEntity(start = jobStart, end = EndUpdate(jobEnd = jobEnd))
        // c.sink.sink(Seq(JobReport.fromEntityToReport(ji) : JobReport))
        jobReports ++= Seq(JobReport.fromEntityToReport(ji) : JobReport)
      } else {
        logger.trace("Ignoring Job end: {}", jobEnd.jobId)
      }

      // purge
      jobStartEvents.remove(jobId)
    }
    c.sink.sink(jobReports)
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
    sqlStartEvents.put(event.executionId, SqlEvent(event.executionId, event.description))
  }

  /** This is the listener method for SQL query end
    *
    * It is a trigger for automatic purge of SQL queries and involved metrics.
    */
  private def onSqlEnd(event: SparkListenerSQLExecutionEnd): Unit = {
    logger.trace("onSqlEnd(...)")

    // get the initial sql collect information (it could have been purged)
    val sqlStartOpt = Option(sqlStartEvents.get(event.executionId))
    var sqlReports: Seq[SqlReport] = Seq()
    sqlStartOpt.foreach { sqlStart =>
      if (c.sqlEnabled) {
        logger.trace("Handling SQL end: {}", event.executionId)
        // generate the SQL input
        val si = SqlEntity(start = sqlStart, end = event)
        // sink the SQL input serialized (as string, and as objects)
        // c.sink.sink(Seq(SqlReport.fromEntityToReport(si) : SqlReport))
        sqlReports ++= Seq(SqlReport.fromEntityToReport(si) : SqlReport)
      } else {
        logger.trace("Ignoring SQL end: {}", event.executionId)
      }

      // purge
      sqlStartEvents.remove(event.executionId)
    }
    c.sink.sink(sqlReports)
  }

  override def onApplicationEnd(event: SparkListenerApplicationEnd): Unit = {
    logger.trace("onApplicationEnd: duration={}", event.time)
    c.sink.flush()
  }
}
