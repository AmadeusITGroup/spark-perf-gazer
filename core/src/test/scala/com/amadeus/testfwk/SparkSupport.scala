package com.amadeus.testfwk

import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

trait SparkSupport {
  val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  val DefaultConfigs: List[(String, String)] =
    List(
      ("spark.sql.shuffle.partitions", "1"),
      ("spark.ui.showConsoleProgress", "false"),
      ("spark.driver.host", "localhost"),
      ("spark.sql.session.timeZone", "UTC"),
      ("spark.sql.adaptive.enabled", "false")
    )
  val UiConfigs: List[(String, String)] =
    List(
      ("spark.ui.enabled", "true")
    )
  val DefaultUiWaitMs: Long = 1000 * 60 * 60 // 1h

  private def clearSparkSessionState(): Unit = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  private def ensureNoActiveSession(appName: String): Unit = {
    val sessions = Seq(
      SparkSession.getActiveSession,
      SparkSession.getDefaultSession
    ).flatten.distinct

    sessions.find(session => !session.sparkContext.isStopped).foreach { session =>
      throw new IllegalStateException(
        s"[$appName] An active Spark session already exists in this JVM: ${session.sparkContext.appName}"
      )
    }

    if (sessions.nonEmpty) {
      clearSparkSessionState()
    }
  }

  private def stopSparkSession(spark: SparkSession): Unit = {
    if (!spark.sparkContext.isStopped) {
      spark.stop()
    }
    clearSparkSessionState()
  }

  def getOrCreateSparkSession(conf: List[(String, String)] = DefaultConfigs, appName: String): SparkSession = {
    val builder = SparkSession.builder
      .appName(appName)
      .master("local[1]")
    val customized = conf.foldLeft(builder) { case (b, (k, v)) =>
      b.config(k, v)
    }

    ensureNoActiveSession(appName)

    customized.getOrCreate()
  }

  private def withSparkInternal[T](
    conf: List[(String, String)],
    name: String
  )(
    testCode: SparkSession => T
  )(
    finallyCode: SparkSession => Unit
  ): T = {
    val spark = getOrCreateSparkSession(conf, name)
    try {
      testCode(spark)
    } finally {
      try {
        finallyCode(spark)
      } finally {
        stopSparkSession(spark)
      }
    }
  }

  def withSpark[T](conf: List[(String, String)] = DefaultConfigs, appName: String = "no name")(
    testCode: SparkSession => T
  ): T = {
    withSparkInternal(conf, appName)(testCode)(_ => ())
  }

  def withSparkAndUi[T](conf: List[(String, String)] = DefaultConfigs, appName: String = "no name")(
    testCode: SparkSession => T
  ): T =
    withSparkInternal(conf ++ UiConfigs, appName)(testCode) { spark =>
      println(s"Launched ${this.getClass.getName}.withSparkUI at ${spark.sparkContext.uiWebUrl}...")
      println("Go to the web Spark UI now. We will wait...")
      Thread.sleep(DefaultUiWaitMs)
      throw new IllegalStateException("The method withSparkUi should only be used in a developer environment")
    }
}
