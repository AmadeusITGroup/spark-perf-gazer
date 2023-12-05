package com.amadeus.sparklear

import com.amadeus.sparklear.prereports.{JobPreReport, PreReport, SqlPreReport, StagePreReport}
import com.amadeus.sparklear.utils.CappedConcurrentHashMap
import com.amadeus.sparklear.collects.JobCollect.EndUpdate
import com.amadeus.sparklear.collects.{JobCollect, SqlCollect, StageCollect}
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.ui._

import java.util.concurrent.ConcurrentHashMap

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
  private val jobCollects = new CappedConcurrentHashMap[JobKey, JobCollect](c.maxCacheSize)
  private val stageCollects = new CappedConcurrentHashMap[StageKey, StageCollect](c.maxCacheSize)
  private val metricCollects = new CappedConcurrentHashMap[MetricKey, MetricCollect](c.maxCacheSize)

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

    // sink the stage input (for testing)
    c.inputSink.foreach(ss => ss(si))

    // sink the stage input serialized (as string, and as objects)
    if (c.showStages) {
      c.outputSink.foreach(ss => c.stageSerializer.toReport(c, si).map(ss))
      c.stringSink.foreach(ss => ss(c.stageSerializer.toStringReport(c, si)))
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
    val jobCollect = jobCollects.get(jobId) // retrieve initial image of job
    val stagesIdAndStats = jobCollect.initialStages.map { sd => // retrieve image of stages
      (sd, Option(stageCollects.get(sd.id)))
    }

    // generate the job input
    val ji = JobPreReport(jobCollect, EndUpdate(finalStages = stagesIdAndStats, jobEnd = jobEnd))

    // sink the job input (for testing)
    c.inputSink.foreach(ss => ss(ji))

    // sink the job input serialized (as string, and as objects)
    if (c.showJobs) {
      c.outputSink.foreach(ss => c.jobSerializer.toReport(c, ji).map(ss))
      c.stringSink.foreach(ss => ss(c.jobSerializer.toStringReport(c, ji)))
    }

    // purge
    jobCollects.remove(jobId)
    val stageIds = jobCollect.initialStages.map(_.id)
    stageIds.foreach(i => stageCollects.remove(i))
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case event: SparkListenerSQLExecutionStart =>
        onSqlStart(event)
      case event: SparkListenerDriverAccumUpdates =>
        // TODO: check if really needed
        event.accumUpdates.foreach{case (k, v) => metricCollects.put(k, v)}
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

    // get the initial sql collect information
    val sqlCollect = sqlCollects.get(event.executionId)

    // generate the SQL input
    val si = SqlPreReport(sqlCollect, metricCollects.toScalaMap ++ m)

    // sink the SQL input (for testing)
    c.inputSink.foreach(ss => ss(si))

    // sink the SQL input serialized (as string, and as objects)
    if (c.showSqls) {
      c.outputSink.foreach(ss => c.sqlSerializer.toReport(c, si).map(ss))
      c.stringSink.foreach(ss => ss(c.sqlSerializer.toStringReport(c, si)))
    }

    // purge
    sqlCollects.remove(event.executionId)
    m.keys.foreach(metricCollects.remove)
  }

}
