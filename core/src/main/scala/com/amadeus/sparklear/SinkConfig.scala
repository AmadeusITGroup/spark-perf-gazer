package com.amadeus.sparklear

import com.amadeus.sparklear.PathBuilder._

sealed trait SinkConfig

final case class JsonSinkConfig(
  destination: String,
  writeBatchSize: Int,
  fileSizeLimit: Long
) extends SinkConfig

object JsonSinkConfig {
  def withDestination(
    destination: String = "/dbfs/tmp/listener/",
    writeBatchSize: Int = 100,
    fileSizeLimit: Long = 1L*1024*1024
  ): JsonSinkConfig = {
    val conf = JsonSinkConfig(
      destination = if (destination.endsWith("/")) destination else destination + "/",
      writeBatchSize = writeBatchSize,
      fileSizeLimit = fileSizeLimit
    )
    conf
  }
  def withBaseDir(
    sparkConf: Map[String, String],
    baseDir: String,
    writeBatchSize: Int = 100,
    fileSizeLimit: Long = 1L*1024*1024
  ): JsonSinkConfig = {
    val conf = JsonSinkConfig(
      destination = baseDir.withDate.withApplicationId(sparkConf),
      writeBatchSize = writeBatchSize,
      fileSizeLimit = fileSizeLimit
    )
    conf
  }
}

final case class LogSinkConfig() extends SinkConfig