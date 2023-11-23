package com.amadeus.sparklear

import com.amadeus.sparklear.reports.Report.StringReport
import com.amadeus.sparklear.reports.{JobReport, Report, SqlReport, StageReport}
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

  type MetricKey = Long
  type MetricValue = Long

  // Maps to keep sqls + jobs + stages wrappers (initial information) until job is completed
  private val sqlWrappers = new ConcurrentHashMap[Long, SqlWrapper]()
  private val jobWrappers = new ConcurrentHashMap[Int, JobWrapper]()
  private val stageWrappers = new ConcurrentHashMap[Int, StageWrapper]()
  private val metrics = new ConcurrentHashMap[MetricKey, MetricValue]()

  def reports: List[Report] = {
    sqlReports ++ jobReports ++ stageReports
  }

  def purge(): Unit = {
    sqlWrappers.clear()
    jobWrappers.clear()
    stageWrappers.clear()
    metrics.clear()
  }

  def stringReports: List[StringReport] = {
    val stringReports = reports.map(_.toStringReport(c))
    stringReports
  }

  private def sqlReports: List[SqlReport] = {
    sqlWrappers.asScala.toList.map {
      case (_, sqlWrapper) =>
        SqlReport(sqlWrapper, metrics.asScala.toMap)
    }
  }

  private def jobReports: List[JobReport] = {
    jobWrappers.asScala.toList.map {
      case (_, jobWrapper) =>
        JobReport(jobWrapper)
    }
  }

  private def stageReports: List[StageReport] = {
    stageWrappers.asScala.toList.map {
      case (_, stageWrapper) =>
        StageReport(stageWrapper)
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
