package com.amadeus.sparklear

import com.amadeus.sparklear.input.{JobInput, Input, SqlInput, StageInput}
import com.amadeus.sparklear.wrappers.JobWrapper.EndUpdate
import com.amadeus.sparklear.wrappers.{JobWrapper, SqlWrapper, StageWrapper}
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.ui._

import scala.collection.JavaConverters
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

  def inputs: List[Input] = {
    def enabled(f: Boolean, r: List[Input]): List[Input] = if (f) r else List.empty[Input]
    enabled(c.showSqls, sqlInputs) ++
      enabled(c.showJobs, jobInputs) ++
      enabled(c.showStages, stageInputs)
  }

  def purgeWrappers(): Unit = {
    sqlWrappers.clear()
    jobWrappers.clear()
    stageWrappers.clear()
    metrics.clear()
  }

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

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    stageWrappers.put(stageCompleted.stageInfo.stageId, StageWrapper(stageCompleted.stageInfo))
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    val jobId = jobEnd.jobId
    val jobWrap = jobWrappers.get(jobId)
    val stagesIdAndStats = jobWrap.initialStages.map { sd =>
      (sd, Option(stageWrappers.get(sd.id)))
    }
    val updatedJobWrap = jobWrap.copy(endUpdate = Some(EndUpdate(finalStages = stagesIdAndStats, jobEnd = jobEnd)))
    jobWrappers.put(jobId, updatedJobWrap)
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    import scala.collection.JavaConverters._
    event match {
      case event: SparkListenerSQLExecutionStart =>
        sqlWrappers.put(event.executionId, SqlWrapper(event.executionId, event.sparkPlanInfo))
      case event: SparkListenerDriverAccumUpdates =>
        metrics.putAll(mapAsJavaMap(event.accumUpdates.toMap))
      case event: SparkListenerSQLAdaptiveSQLMetricUpdates =>
      // ignore for now, adds more metrics
      case event: SparkListenerSQLAdaptiveExecutionUpdate =>
        sqlWrappers.put(event.executionId, SqlWrapper(event.executionId, event.sparkPlanInfo))
      case event: SparkListenerSQLExecutionEnd =>
        val plan = sqlWrappers.get(event.executionId)
      //display(s"SQL Query completed: id=${event.executionId}, ${SparkPlanInfoPrettifier.prettify(plan, metrics, "  ")}")
      case _ =>
    }
  }

}
