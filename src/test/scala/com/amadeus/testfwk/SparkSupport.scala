package com.amadeus.testfwk

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

trait SparkSupport {
  lazy val logger: Logger = Logger(LoggerFactory.getLogger(getClass.getName))

  val DefaultConfigs: List[(String, String)] =
    List(
      ("spark.sql.shuffle.partitions", "1"),
      ("spark.ui.showConsoleProgress", "false"),
      ("spark.driver.host", "localhost"),
      ("spark.sql.session.timeZone", "UTC"),
      ("spark.sql.adaptive.enabled", "false")
    )
  def getOrCreateSparkSession(conf: List[(String, String)] = DefaultConfigs): SparkSession = {
    val builder = SparkSession.builder
      .appName("SparkSession for tests")
      .master("local[1]")
    val customized = conf.foldLeft(builder) { case (b, (k, v)) =>
      b.config(k, v)
    }
    customized.getOrCreate()
  }

  def withSpark[T](conf: List[(String, String)] = DefaultConfigs)(testCode: SparkSession => T): T = {
    val spark = getOrCreateSparkSession(conf)
    try {
      testCode(spark)
    } finally {
      spark.stop()
    }
  }
}