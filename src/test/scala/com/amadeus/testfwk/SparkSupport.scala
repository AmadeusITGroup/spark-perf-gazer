package com.amadeus.testfwk

import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

trait SparkSupport {
  lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

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
  val DefaultUiWaitMs = 1000 * 60 * 60 // 1h

  def getOrCreateSparkSession(conf: List[(String, String)] = DefaultConfigs): SparkSession = {
    val builder = SparkSession.builder
      .appName("SparkSession for tests")
      .master("local[1]")
    val customized = conf.foldLeft(builder) { case (b, (k, v)) =>
      b.config(k, v)
    }
    customized.getOrCreate()
  }

  private def withSparkInternal[T](
    conf: List[(String, String)]
  )(
    testCode: SparkSession => T
  )(
    finallyCode: SparkSession => Unit
  ): T = {
    val spark = getOrCreateSparkSession(conf)
    try {
      testCode(spark)
    } finally {
      finallyCode(spark)
    }
  }

  def withSpark[T](conf: List[(String, String)] = DefaultConfigs)(testCode: SparkSession => T): T = {
    // withSparkInternal(conf)(testCode)(_.stop())
    withSparkInternal(conf)(testCode) { spark =>
      spark.stop()
    }
  }

  def withSparkAndUi[T](conf: List[(String, String)] = DefaultConfigs)(testCode: SparkSession => T): T =
    withSparkInternal(conf ++ UiConfigs)(testCode) { spark =>
      println(s"Launched ${this.getClass.getName}.withSparkUI at ${spark.sparkContext.uiWebUrl}...")
      println("Go to the web Spark UI now. We will wait...")
      Thread.sleep(DefaultUiWaitMs)
      spark.stop()
      throw new IllegalStateException("The method withSparkUi should only be used in a developer environment")
    }
}
