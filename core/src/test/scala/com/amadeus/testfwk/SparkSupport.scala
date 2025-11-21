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

  def getOrCreateSparkSession(conf: List[(String, String)] = DefaultConfigs, appName: String): SparkSession = {
    val builder = SparkSession.builder
      .appName(appName)
      .master("local[1]")
    val customized = conf.foldLeft(builder) { case (b, (k, v)) =>
      b.config(k, v)
    }

    SparkSession.getActiveSession.foreach { s =>
      throw new IllegalStateException(
        s"[$appName] An active Spark session already exists in this JVM: ${s.sparkContext.appName}"
      )
    }

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
    val r =
      try {
        testCode(spark)
      } finally {
        finallyCode(spark)
      }
    spark.stop()
    r
  }

  def withSpark[T](conf: List[(String, String)] = DefaultConfigs, appName: String = "no name")(
    testCode: SparkSession => T
  ): T = {
    withSparkInternal(conf, appName)(testCode)(_.stop())
  }

  def withSparkAndUi[T](conf: List[(String, String)] = DefaultConfigs, appName: String = "no name")(
    testCode: SparkSession => T
  ): T =
    withSparkInternal(conf ++ UiConfigs, appName)(testCode) { spark =>
      println(s"Launched ${this.getClass.getName}.withSparkUI at ${spark.sparkContext.uiWebUrl}...")
      println("Go to the web Spark UI now. We will wait...")
      Thread.sleep(DefaultUiWaitMs)
      spark.stop()
      throw new IllegalStateException("The method withSparkUi should only be used in a developer environment")
    }
}
