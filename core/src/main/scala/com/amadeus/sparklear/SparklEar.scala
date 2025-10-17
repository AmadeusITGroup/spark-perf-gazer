package com.amadeus.sparklear

import com.amadeus.sparklear.SparklEar.{configFrom, sinkFrom}
import com.amadeus.sparklear.entities.{JobEntity, SqlEntity, StageEntity, TaskEntity}
import com.amadeus.sparklear.events.JobEvent.EndUpdate
import com.amadeus.sparklear.events.{JobEvent, SqlEvent, StageEvent, TaskEvent}
import com.amadeus.sparklear.reports.{JobReport, SqlReport, StageReport, TaskReport}
import com.amadeus.sparklear.utils.CappedConcurrentHashMap
import org.apache.spark.SparkConf
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.ui._
import org.slf4j.{Logger, LoggerFactory}

/** This listener displays in the Spark Driver STDOUT some
  * relevant information about the application, including:
  * - total executor CPU time
  * - spilled tasks
  * - ...
  */
class SparklEar(c: SparklearConfig, sink: Sink) extends SparkListener {

  implicit lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  def this(sparkConf: SparkConf) = {
    this(c = configFrom(sparkConf), sinkFrom(sparkConf))
  }

  type SqlKey = Long
  type JobKey = Int

  // Maps to keep sqls + jobs + stages raw events (initial information) until some completion
  private val sqlStartEvents = new CappedConcurrentHashMap[SqlKey, SqlEvent](c.maxCacheSize)
  private val jobStartEvents = new CappedConcurrentHashMap[JobKey, JobEvent](c.maxCacheSize)

  // Register shutdown hook to ensure sink is closed on JVM termination
  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = {
      logger.info("Shutdown hook triggered, closing sink")
      sink.close()
    }
  })

  /** LISTENERS
    */

  /** This is the listener method for stage end
    *
    * It is NOT a trigger for automatic purge of stages (job end will purge stages).
    */
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    if (c.tasksEnabled) {
      logger.trace("onTaskEnd(...) id = {}", taskEnd.taskInfo.taskId)
      val te = TaskEvent(taskEnd)
      val ti = TaskEntity(te)
      sink.write(TaskReport.fromEntityToReport(ti): TaskReport)
    }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    if (c.stagesEnabled) {
      logger.trace("onStageCompleted(...) id = {}", stageCompleted.stageInfo.stageId)
      val sw = StageEvent(stageCompleted.stageInfo)
      val si = StageEntity(sw)
      sink.write(StageReport.fromEntityToReport(si): StageReport)
    }
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    if (c.jobsEnabled) {
      jobStartEvents.put(jobStart.jobId, JobEvent.from(jobStart))
      logger.trace("onJobStart(...) id = {} (size of map {})", jobStart.jobId, jobStartEvents.size)
    }
  }

  /** This is the listener method for job end
    *
    * It is a trigger for automatic purge of job and stages.
    */
  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    if (c.jobsEnabled) {
      logger.trace("onJobEnd(...), id = {}", jobEnd.jobId)
      val jobStartOpt = Option(jobStartEvents.get(jobEnd.jobId)) // retrieve initial image of job
      jobStartOpt match {
        case Some(jobStart) =>
          val ji = JobEntity(start = jobStart, end = EndUpdate(jobEnd = jobEnd))
          sink.write(JobReport.fromEntityToReport(ji))
          jobStartEvents.remove(jobEnd.jobId)
        case None =>
          logger.warn("Job start event not found for jobId: {}", jobEnd.jobId)
      }
    }
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
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
    if (c.sqlEnabled) {
      sqlStartEvents.put(event.executionId, SqlEvent(event.executionId, event.description))
      logger.trace("onSqlStart(...) id = {} (size of map {})", event.executionId, sqlStartEvents.size)
    }
  }

  /** This is the listener method for SQL query end
    *
    * It is a trigger for automatic purge of SQL queries and involved metrics.
    */
  private def onSqlEnd(event: SparkListenerSQLExecutionEnd): Unit = {
    if (c.sqlEnabled) {
      logger.trace("onSqlEnd(...) id = {}", event.executionId)
      val sqlStartOpt = Option(sqlStartEvents.get(event.executionId))
      sqlStartOpt match {
        case Some(sqlStart) =>
          val si = SqlEntity(start = sqlStart, end = event)
          sink.write(SqlReport.fromEntityToReport(si))
          sqlStartEvents.remove(event.executionId)
        case None =>
          logger.warn("SQL start event not found for executionId: {}", event.executionId)
      }
    }
  }

  override def onApplicationEnd(event: SparkListenerApplicationEnd): Unit = {
    logger.trace("onApplicationEnd: end={}", event.time)
    this.close()
  }

  /**
    * Close the sink (if not already done).
    */
  def close(): Unit = {
    logSnippets()
    sink.close()
    logger.info("Listener closed, size of maps sql={} and job={})",
      sqlStartEvents.size, jobStartEvents.size)
  }

  private def logSnippets(): Unit = {
    Seq("sql", "job", "stage", "task").foreach { reportType =>
      val ddl = sink.generateViewSnippet(reportType)
      logger.info(s"To create a temporary view for $reportType reports, run the following SQL:\n${ddl}")
    }
  }

}

object SparklEar {
  val SinkClassKey = "spark.sparklear.sink.class"

  implicit lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  def configFrom(sparkConf: SparkConf): SparklearConfig = {
    val conf = SparklearConfig(sparkConf)
    logger.info(s"Sparklear config: $conf")
    conf
  }

  def sinkFrom(sparkConf: SparkConf): Sink = {
    val sinkClassNameOption = sparkConf.getOption(SinkClassKey)
    val sink = sinkClassNameOption match {
      case Some(sinkClassName) =>
        // call sink class constructor with sparkConf
        val params = classOf[SparkConf]
        val sinkClass = Class.forName(sinkClassName)
        val constructor = sinkClass.getDeclaredConstructor(params)
        val sink = constructor.newInstance(sparkConf).asInstanceOf[Sink]
        sink
      case None =>
        throw new IllegalArgumentException(SinkClassKey + " is not set")
    }
    logger.info(s"Sink: ${sink.asString}")
    sink
  }
}
