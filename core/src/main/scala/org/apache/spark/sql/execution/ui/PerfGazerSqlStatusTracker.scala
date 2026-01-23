package org.apache.spark.sql.execution.ui

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.Status.ASYNC_TRACKING_ENABLED
import org.apache.spark.scheduler._
import org.apache.spark.status.ElementTrackingStore
import org.apache.spark.util.kvstore.InMemoryStore

class PerfGazerSqlStatusTracker(conf: SparkConf, live: Boolean) {
  private val trackerConf = PerfGazerSqlStatusTracker.copyConf(conf)
  private val kvstore = new ElementTrackingStore(new InMemoryStore, trackerConf)
  private val listener = new SQLAppStatusListener(trackerConf, kvstore, live)
  private val store = new SQLAppStatusStore(kvstore, Some(listener))

  def onJobStart(event: SparkListenerJobStart): Unit = listener.onJobStart(event)

  def onJobEnd(event: SparkListenerJobEnd): Unit = listener.onJobEnd(event)

  def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = listener.onStageSubmitted(event)

  def onTaskStart(event: SparkListenerTaskStart): Unit = listener.onTaskStart(event)

  def onTaskEnd(event: SparkListenerTaskEnd): Unit = listener.onTaskEnd(event)

  def onExecutorMetricsUpdate(event: SparkListenerExecutorMetricsUpdate): Unit =
    listener.onExecutorMetricsUpdate(event)

  def onOtherEvent(event: SparkListenerEvent): Unit = listener.onOtherEvent(event)

  def executionMetrics(executionId: Long): Map[Long, String] = store.executionMetrics(executionId)

  def close(): Unit = kvstore.close()
}

object PerfGazerSqlStatusTracker {
  private def copyConf(conf: SparkConf): SparkConf = {
    val trackerConf = new SparkConf(false).setAll(conf.getAll)
    // Ensure SQL metrics are available synchronously when the execution end event arrives.
    trackerConf.set(ASYNC_TRACKING_ENABLED, false)
    trackerConf
  }
}
