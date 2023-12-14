package com.amadeus.sparklear

import com.amadeus.sparklear.prereports.{JobPreReport, PreReport, SqlPreReport, StagePreReport}
import com.amadeus.sparklear.utils.CappedConcurrentHashMap
import com.amadeus.sparklear.collects.JobCollect.EndUpdate
import com.amadeus.sparklear.collects.{JobCollect, SqlCollect, StageCollect}
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

  // Maps to keep sqls + jobs + stages collects (initial information) until some completion
  private val sqlCollects = new CappedConcurrentHashMap[SqlKey, SqlCollect](c.maxCacheSize)
  private val jobCollects = new CappedConcurrentHashMap[JobKey, JobCollect](c.maxCacheSize)
  private val stageCollects = new CappedConcurrentHashMap[StageKey, StageCollect](c.maxCacheSize)

  /** LISTENERS
    */

  /** This is the listener method for stage end
    *
    * It is NOT a trigger for automatic purge of stages (job end will purge stages).
    */
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    logger.trace("onStageCompleted(...)")
    // generate a stage collect
    val sw = StageCollect(stageCompleted.stageInfo)

    // store stage collect for the use in jobs
    stageCollects.put(stageCompleted.stageInfo.stageId, sw)

    if (c.showStages) {
      logger.trace(s"Handling Stage end: ${stageCompleted.stageInfo.stageId}")
      // generate the stage input
      val si = StagePreReport(sw)
      // sink the stage input (for testing)
      c.preReportSink.foreach(ss => ss(si))
      // sink the stage input serialized (as string, and as objects)
      c.reportSink.foreach(ss => c.stageTranslator.toReports(c, si).map(ss))
      c.stringReportSink.foreach(ss => c.stageTranslator.toStringReports(c, si).map(ss))
    } else {
      logger.trace(s"Ignoring Stage end: ${stageCompleted.stageInfo.stageId}")
    }

    // nothing to purge
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    logger.trace("onJobStart(...)")
    jobCollects.put(jobStart.jobId, JobCollect.from(jobStart))
  }

  /** This is the listener method for job end
    *
    * It is a trigger for automatic purge of job and stages.
    */
  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    logger.trace("onJobEnd(...)")
    val jobId = jobEnd.jobId
    val jobCollectOpt = Option(jobCollects.get(jobId)) // retrieve initial image of job (it could have been purged)
    jobCollectOpt.foreach { jobCollect =>
      val stagesIdAndStats = jobCollect.initialStages.map { sd => // retrieve image of stages
        (sd, Option(stageCollects.get(sd.id)))
      }

      if (c.showJobs) {
        logger.trace(s"Handling Job end: ${jobEnd.jobId}")
        // generate the job input
        val ji = JobPreReport(jobCollect, EndUpdate(finalStages = stagesIdAndStats, jobEnd = jobEnd))
        // sink the job input (for testing)
        c.preReportSink.foreach(ss => ss(ji))
        // sink the job input serialized (as string, and as objects)
        c.reportSink.foreach(ss => c.jobTranslator.toReports(c, ji).map(ss))
        c.stringReportSink.foreach(ss => c.jobTranslator.toStringReports(c, ji).map(ss))
      } else {
        logger.trace(s"Ignoring Job end: ${jobEnd.jobId}")
      }

      // purge
      jobCollects.remove(jobId)
      val stageIds = jobCollect.initialStages.map(_.id)
      stageIds.foreach(i => stageCollects.remove(i))
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
        logger.trace(s"Event ignored: ${e.getClass.getName}")
      //case _: SparkListenerSQLAdaptiveSQLMetricUpdates =>
      // TODO: ignored for now, maybe adds more metrics?
      //case _: SparkListenerSQLAdaptiveExecutionUpdate =>
      // TODO: ignored for now, maybe adds more metrics?
    }
  }

  private def onSqlStart(event: SparkListenerSQLExecutionStart): Unit = {
    logger.trace("onSqlStart(...)")
    sqlCollects.put(event.executionId, SqlCollect(event.executionId, event.sparkPlanInfo, event.description))
  }

  /** This is the listener method for SQL query end
    *
    * It is a trigger for automatic purge of SQL queries and involved metrics.
    */
  private def onSqlEnd(event: SparkListenerSQLExecutionEnd): Unit = {
    logger.trace("onSqlEnd(...)")

    // get the initial sql collect information (it could have been purged)
    val sqlCollectOpt = Option(sqlCollects.get(event.executionId))

    sqlCollectOpt.foreach { sqlCollect =>
      if (c.showSqls) {
        logger.trace(s"Handling SQL end: ${event.executionId}")
        // generate the SQL input
        val si = SqlPreReport(sqlCollect, event)
        // sink the SQL input (for testing)
        c.preReportSink.foreach(ss => ss(si))
        // sink the SQL input serialized (as string, and as objects)
        c.reportSink.foreach(ss => c.sqlTranslator.toReports(c, si).map(ss))
        c.stringReportSink.foreach(ss => c.sqlTranslator.toStringReports(c, si).map(ss))
      } else {
        logger.trace(s"Ignoring SQL end: ${event.executionId}")
      }

      // purge
      sqlCollects.remove(event.executionId)
    }
  }

}
