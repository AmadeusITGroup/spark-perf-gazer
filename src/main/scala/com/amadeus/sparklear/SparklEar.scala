package com.amadeus.sparklear

import com.amadeus.sparklear.prereports.{JobPreReport, PreReport, SqlPreReport, StagePreReport}
import com.amadeus.sparklear.utils.CappedConcurrentHashMap
import com.amadeus.sparklear.collects.JobCollect.EndUpdate
import com.amadeus.sparklear.collects.{JobCollect, SqlCollect, StageCollect}
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.ui._


/** This listener displays in the Spark Driver STDOUT some
  * relevant information about the application, including:
  * - total executor CPU time
  * - spilled tasks
  * - ...
  */
class SparklEar(c: Config) extends SparkListener {

  type SqlKey = Long
  type JobKey = Int
  type StageKey = Int
  type MetricKey = Long
  type MetricCollect = Long

  // Maps to keep sqls + jobs + stages collects (initial information) until some completion
  private val sqlCollects = new CappedConcurrentHashMap[SqlKey, SqlCollect](c.maxCacheSize)
  private val sqlMetricCollects = new CappedConcurrentHashMap[MetricKey, MetricCollect](c.maxCacheSize)
  private val jobCollects = new CappedConcurrentHashMap[JobKey, JobCollect](c.maxCacheSize)
  private val stageCollects = new CappedConcurrentHashMap[StageKey, StageCollect](c.maxCacheSize)

  /** LISTENERS
    */

  /**
    * This is the listener method for stage end
    *
    * It is NOT a trigger for automatic purge of stages (job end will purge stages).
    */
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    // generate a stage collect
    val sw = StageCollect(stageCompleted.stageInfo)

    // store stage collect for the use in jobs
    stageCollects.put(stageCompleted.stageInfo.stageId, sw)

    // generate the stage input
    val si = StagePreReport(sw)

    if (c.showStages) {
      // sink the stage input (for testing)
      c.preReportSink.foreach(ss => ss(si))
      // sink the stage input serialized (as string, and as objects)
      c.reportSink.foreach(ss => c.stageTranslator.toReports(c, si).map(ss))
      c.stringReportSink.foreach(ss => c.stageTranslator.toStringReports(c, si).map(ss))
    }

    // nothing to purge
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    jobCollects.put(jobStart.jobId, JobCollect.from(jobStart))
  }

  /**
    * This is the listener method for job end
    *
    * It is a trigger for automatic purge of job and stages.
    */
  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    val jobId = jobEnd.jobId
    val jobCollectOpt = Option(jobCollects.get(jobId)) // retrieve initial image of job (it could have been purged)
    jobCollectOpt.foreach { jobCollect =>
      val stagesIdAndStats = jobCollect.initialStages.map { sd => // retrieve image of stages
        (sd, Option(stageCollects.get(sd.id)))
      }

      // generate the job input
      val ji = JobPreReport(jobCollect, EndUpdate(finalStages = stagesIdAndStats, jobEnd = jobEnd))

      if (c.showJobs) {
        // sink the job input (for testing)
        c.preReportSink.foreach(ss => ss(ji))
        // sink the job input serialized (as string, and as objects)
        c.reportSink.foreach(ss => c.jobTranslator.toReports(c, ji).map(ss))
        c.stringReportSink.foreach(ss => c.jobTranslator.toStringReports(c, ji).map(ss))
      }

      // purge
      jobCollects.remove(jobId)
      val stageIds = jobCollect.initialStages.map(_.id)
      stageIds.foreach(i => stageCollects.remove(i))
    }
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case event: SparkListenerSQLExecutionStart =>
        onSqlStart(event)
      case event: SparkListenerDriverAccumUpdates =>
        // TODO: check if really needed
        event.accumUpdates.foreach{case (k, v) => sqlMetricCollects.put(k, v)}
      case _: SparkListenerSQLAdaptiveSQLMetricUpdates =>
        // TODO: ignored for now, maybe adds more metrics?
      case _: SparkListenerSQLAdaptiveExecutionUpdate =>
        // TODO: ignored for now, maybe adds more metrics?
        //sqlCollects.put(event.executionId, SqlCollect(event.executionId, event.sparkPlanInfo, event.))
      case event: SparkListenerSQLExecutionEnd =>
        onSqlEnd(event)
      case _ =>
    }
  }

  private def onSqlStart(event: SparkListenerSQLExecutionStart): Unit = {
    sqlCollects.put(event.executionId, SqlCollect(event.executionId, event.sparkPlanInfo, event.description))
  }


  /**
    * This is the listener method for SQL query end
    *
    * It is a trigger for automatic purge of SQL queries and involved metrics.
    */
  private def onSqlEnd(event: SparkListenerSQLExecutionEnd): Unit = {
    val m = SparkInternal.executedPlanMetrics(event)

    // get the initial sql collect information (it could have been purged)
    val sqlCollectOpt = Option(sqlCollects.get(event.executionId))

    sqlCollectOpt.foreach { sqlCollect =>
      // generate the SQL input
      val si = SqlPreReport(sqlCollect, sqlMetricCollects.toScalaMap ++ m)

      if (c.showSqls) {
        // sink the SQL input (for testing)
        c.preReportSink.foreach(ss => ss(si))
        // sink the SQL input serialized (as string, and as objects)
        c.reportSink.foreach(ss => c.sqlTranslator.toReports(c, si).map(ss))
        c.stringReportSink.foreach(ss => c.sqlTranslator.toStringReports(c, si).map(ss))
      }

      // purge
      sqlCollects.remove(event.executionId)
      m.keys.foreach(sqlMetricCollects.remove)
    }
  }

}
