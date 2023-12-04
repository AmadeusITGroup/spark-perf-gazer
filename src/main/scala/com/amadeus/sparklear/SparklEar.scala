package com.amadeus.sparklear

import com.amadeus.sparklear.input.{JobInput, Input, SqlInput, StageInput}
import com.amadeus.sparklear.wrappers.JobWrapper.EndUpdate
import com.amadeus.sparklear.wrappers.{JobWrapper, SqlWrapper, StageWrapper}
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
  type MetricWrapper = Long


  // Maps to keep sqls + jobs + stages wrappers (initial information) until some completion
  private val sqlWrappers = new ConcurrentHashMap[SqlKey, SqlWrapper](c.maxCacheSize)
  private val jobWrappers = new ConcurrentHashMap[JobKey, JobWrapper](c.maxCacheSize)
  private val stageWrappers = new ConcurrentHashMap[StageKey, StageWrapper](c.maxCacheSize)
  private val metricWrappers = new ConcurrentHashMap[MetricKey, MetricWrapper](c.maxCacheSize)

  private def cappedSqlWrapperPut(k: SqlKey, v: SqlWrapper) = {
    import scala.collection.JavaConverters._
    if (sqlWrappers.size() - 1 > c.maxCacheSize) {
      sqlWrappers.remove(sqlWrappers.keys().asScala.min)
    }
    sqlWrappers.put(k, v)
  }

  private def cappedJobWrapperPut(k: JobKey, v: JobWrapper) = {
    import scala.collection.JavaConverters._
    if (jobWrappers.size() - 1 > c.maxCacheSize) {
      jobWrappers.remove(jobWrappers.keys().asScala.min)
    }
    jobWrappers.put(k, v)
  }

  private def cappedStageWrapperPut(k: StageKey, v: StageWrapper) = {
    import scala.collection.JavaConverters._
    if (stageWrappers.size() - 1 > c.maxCacheSize) {
      stageWrappers.remove(stageWrappers.keys().asScala.min)
    }
    stageWrappers.put(k, v)
  }

  private def cappedMetricWrapperPut(k: MetricKey, v: MetricWrapper) = {
    import scala.collection.JavaConverters._
    if (metricWrappers.size() - 1 > c.maxCacheSize) {
      metricWrappers.remove(metricWrappers.keys().asScala.min)
    }
    metricWrappers.put(k, v)
  }

  /**
    * Return the number of wrappers not purged from memory
    */
  def unpurged: Long = {
    sqlWrappers.size() + jobWrappers.size() + stageWrappers.size() + metricWrappers.size()
  }

  /** LISTENERS
    */

  /**
    * This is the listener method for stage end
    *
    * It is NOT a trigger for automatic purge of stages (job end will purge stages).
    */
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    // generate a stage wrapper
    val sw = StageWrapper(stageCompleted.stageInfo)

    // store stage wrapper for the use in jobs
    cappedStageWrapperPut(stageCompleted.stageInfo.stageId, sw)

    // generate the stage input
    val si = StageInput(sw)

    // sink the stage input (for testing)
    c.inputSink.foreach(ss => ss(si))

    // sink the stage input serialized (as string, and as objects)
    if (c.showStages) {
      c.stringSink.foreach(ss => ss(c.stageSerializer.toStringReport(c, si)))
      c.outputSink.foreach(ss => c.stageSerializer.toOutput(c, si).map(ss))
    }

    // nothing to purge
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    cappedJobWrapperPut(jobStart.jobId, JobWrapper.from(jobStart))
  }

  /**
    * This is the listener method for job end
    *
    * It is a trigger for automatic purge of job and stages.
    */
  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    val jobId = jobEnd.jobId
    val jobWrap = jobWrappers.get(jobId) // retrieve initial image of job
    val stagesIdAndStats = jobWrap.initialStages.map { sd => // retrieve image of stages
      (sd, Option(stageWrappers.get(sd.id)))
    }

    // generate an updated job wrapper with updated job info, and stages info
    val updatedJobWrap = jobWrap.copy(endUpdate = Some(EndUpdate(finalStages = stagesIdAndStats, jobEnd = jobEnd)))
    // generate the job input
    val ji = JobInput(updatedJobWrap)

    // sink the job input (for testing)
    c.inputSink.foreach(ss => ss(ji))

    // sink the job input serialized (as string, and as objects)
    if (c.showJobs) {
      c.stringSink.foreach(ss => ss(c.jobSerializer.toStringReport(c, ji)))
      c.outputSink.foreach(ss => c.jobSerializer.toOutput(c, ji).map(ss))
    }

    // purge
    jobWrappers.remove(jobId)
    val stageIds = jobWrap.initialStages.map(_.id)
    stageIds.foreach(i => stageWrappers.remove(i))
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case event: SparkListenerSQLExecutionStart =>
        cappedSqlWrapperPut(event.executionId, SqlWrapper(event.executionId, event.sparkPlanInfo))
      case event: SparkListenerDriverAccumUpdates =>
        // TODO: check if really needed
        event.accumUpdates.foreach{case (k, v) => cappedMetricWrapperPut(k, v)}
      case _: SparkListenerSQLAdaptiveSQLMetricUpdates =>
        // TODO: ignored for now, maybe adds more metrics?
      case event: SparkListenerSQLAdaptiveExecutionUpdate =>
        cappedSqlWrapperPut(event.executionId, SqlWrapper(event.executionId, event.sparkPlanInfo))
      case event: SparkListenerSQLExecutionEnd =>
        onSqlEnd(event)
      case _ =>
    }
  }

  /**
    * This is the listener method for SQL query end
    *
    * It is a trigger for automatic purge of SQL queries and involved metrics.
    */
  private def onSqlEnd(event: SparkListenerSQLExecutionEnd): Unit = {
    import scala.collection.JavaConverters._
    val m = SparkInternal.executedPlanMetrics(event)

    // get the initial sql wrapper information
    val sqlWrapper = sqlWrappers.get(event.executionId)

    // generate the SQL input
    val si = SqlInput(sqlWrapper, metricWrappers.asScala.toMap ++ m)

    // sink the SQL input (for testing)
    c.inputSink.foreach(ss => ss(si))

    // sink the SQL input serialized (as string, and as objects)
    if (c.showSqls) {
      c.stringSink.foreach(ss => ss(c.sqlSerializer.toStringReport(c, si)))
      c.outputSink.foreach(ss => c.sqlSerializer.toOutput(c, si).map(ss))
    }

    // purge
    sqlWrappers.remove(event.executionId)
    m.keys.foreach(metricWrappers.remove)
  }

}
