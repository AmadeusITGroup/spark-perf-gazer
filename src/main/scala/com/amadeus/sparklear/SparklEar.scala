package com.amadeus.sparklear

import com.amadeus.sparklear.input.{JobInput, Input, SqlInput, StageInput}
import com.amadeus.sparklear.wrappers.JobWrapper.EndUpdate
import com.amadeus.sparklear.wrappers.{JobWrapper, SqlWrapper, StageWrapper}
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.ui._

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters.mapAsScalaConcurrentMapConverter

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

  // Maps to keep sqls + jobs + stages wrappers (initial information) until job is completed
  private val sqlWrappers = new ConcurrentHashMap[SqlKey, SqlWrapper]()
  private val jobWrappers = new ConcurrentHashMap[JobKey, JobWrapper]()
  private val stageWrappers = new ConcurrentHashMap[StageKey, StageWrapper]()
  private val metrics = new ConcurrentHashMap[MetricKey, MetricWrapper]()

  private def sqlInputs: List[SqlInput] = {
    sqlWrappers.asScala.toList.map {
      case (_, sqlWrapper) =>
        SqlInput(sqlWrapper, metrics.asScala.toMap)
    }
  }

  private def jobInputs: List[JobInput] = {
    jobWrappers.asScala.toList.map {
      case (_, jobWrapper) =>
        JobInput(jobWrapper)
    }
  }

  private def stageInputs: List[StageInput] = {
    stageWrappers.asScala.toList.map {
      case (_, stageWrapper) =>
        StageInput(stageWrapper)
    }
  }

  /** LISTENERS
    */

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    jobWrappers.put(jobStart.jobId, JobWrapper.from(jobStart))
  }

  /**
    * This is the listener method for stage end
    *
    * It is NOT a trigger for automatic purge of stages (job end will purge stages).
    */
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    // generate a stage wrapper
    val sw = StageWrapper(stageCompleted.stageInfo)

    // store stage wrapper for the use in jobs
    stageWrappers.put(stageCompleted.stageInfo.stageId, sw)

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
    stagesIdAndStats.foreach(i => stageWrappers.remove(i))
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    import scala.collection.JavaConverters._
    event match {
      case event: SparkListenerSQLExecutionStart =>
        sqlWrappers.put(event.executionId, SqlWrapper(event.executionId, event.sparkPlanInfo))
      case event: SparkListenerDriverAccumUpdates =>
        metrics.putAll(mapAsJavaMap(event.accumUpdates.toMap))
      case _: SparkListenerSQLAdaptiveSQLMetricUpdates =>
      // TODO: ignored for now, maybe adds more metrics?
      case event: SparkListenerSQLAdaptiveExecutionUpdate =>
        sqlWrappers.put(event.executionId, SqlWrapper(event.executionId, event.sparkPlanInfo))
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
    val si = SqlInput(sqlWrapper, metrics.asScala.toMap ++ m)

    // sink the SQL input (for testing)
    c.inputSink.foreach(ss => ss(si))

    // sink the SQL input serialized (as string, and as objects)
    if (c.showSqls) {
      c.stringSink.foreach(ss => ss(c.sqlSerializer.toStringReport(c, si)))
      c.outputSink.foreach(ss => c.sqlSerializer.toOutput(c, si).map(ss))
    }

    // purge
    sqlWrappers.remove(event.executionId)
    m.keys.foreach(metrics.remove)
  }

}
