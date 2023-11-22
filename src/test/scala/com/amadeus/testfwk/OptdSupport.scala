package com.amadeus.testfwk

import org.apache.spark.sql.{DataFrame, SparkSession}

trait OptdSupport {
  def readOptd(spark: SparkSession): DataFrame = {
    spark
      .read
      .option("delimiter", "^")
      .option("header", "true")
      .csv("src/test/resources/optd_por_public_all.csv")
  }

}
