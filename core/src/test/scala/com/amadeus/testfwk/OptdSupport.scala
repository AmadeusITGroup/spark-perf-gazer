package com.amadeus.testfwk

import org.apache.spark.sql.{DataFrame, SparkSession}

object OptdSupport {
  def readOptd(spark: SparkSession): DataFrame = {
    spark
      .read
      .option("delimiter", "^")
      .option("header", "true")
      .csv(getClass.getResource("/optd_por_public_all.csv").getPath)
  }
}
