package com.amadeus.sparklear

import com.amadeus.sparklear.reports.Report.StringReport
import com.amadeus.sparklear.reports.{JobReport, Report, SqlReport, StageReport}
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
    jobWrappers.put(jobStart.jobId, JobWrapper.from(jobStart.stageInfos, jobStart.properties))
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    stageWrappers.put(stageCompleted.stageInfo.stageId, StageWrapper(stageCompleted.stageInfo))
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    val jobId = jobEnd.jobId
    val jobDesc = jobWrappers.get(jobId)
    val stagesIdAndStats = jobDesc.stages.map { sd =>
      (sd, Option(stageWrappers.get(sd.id)))
    }
    val totalExecCpuTimeSec = stagesIdAndStats.collect { case (_, Some(stgStats)) => stgStats.execCpuSecs }.sum

    val spillMb = stagesIdAndStats.collect { case (_, Some(stgStats)) => stgStats.spillMb }.flatten.sum
    val spillReport = if (spillMb != 0) s"SPILL_MB=$spillMb" else ""

    val header =
      s"JOB ID=${jobEnd.jobId} GROUP='${jobDesc.group}' NAME='${jobDesc.name}' SQL_ID=${jobDesc.sqlId} ${spillReport}"
    val jobStats =
      s"STAGES=${jobDesc.stages.size} TOTAL_CPU_SEC=${totalExecCpuTimeSec}"
    val stagesStats =
      if (!c.reportStageDetails) ""
      else
        "\n" + stagesIdAndStats
          .map { case (id, stgStats) =>
            s"- STAGE JOB=${jobId} ${id}: ${stgStats.mkString}"
          }
          .mkString("\n")

    //display(s"$header $jobStats $stagesStats")

    // Keep maps small
    jobWrappers.remove(jobId)
    jobDesc.stages.foreach(sd => stageWrappers.remove(sd.id))
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    // https://docs.databricks.com/en/clusters/configure.html#cluster-log-delivery
    //For example, if the log path is dbfs:/cluster-logs, the log files for a specific cluster will be stored in dbfs:/cluster-logs/<cluster-name> and the individual event logs will be stored in dbfs:/cluster-logs/<cluster-name>/eventlog/<cluster-name-cluster-ip>/<log-id>/.
    // spark.eventLog.enabled true
    // spark.eventLog.dir dbfs:/databricks/unravel/eventLogs/
    // spark.eventLog.enabled true
    // spark.eventLog.dir hdfs://namenode/shared/spark-logs
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
