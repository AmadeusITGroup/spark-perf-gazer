package com.amadeus.sparklear

object SparklearSparkConf {
  val SqlEnabledKey = "spark.sparklear.sql.enabled"
  val JobsEnabledKey = "spark.sparklear.jobs.enabled"
  val StagesEnabledKey = "spark.sparklear.stages.enabled"
  val TasksEnabledKey = "spark.sparklear.tasks.enabled"
  val MaxCacheSizeKey = "spark.sparklear.max.cache.size"

  val SinkClassKey = "spark.sparklear.sink.class"
  val JsonSinkDestinationKey = "spark.sparklear.sink.json.destination"
  val JsonSinkPartitionsKey = "spark.sparklear.sink.json.partitions"
  val JsonSinkWriteBatchSizeKey = "spark.sparklear.sink.json.writeBatchSize"
  val JsonSinkFileSizeLimitKey = "spark.sparklear.sink.json.fileSizeLimit"
}