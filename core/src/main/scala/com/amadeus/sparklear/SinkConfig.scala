package com.amadeus.sparklear

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import PathBuilder._

sealed trait SinkConfig

final case class JsonSinkConfig(
  destination: String,
  writeBatchSize: Int,
  fileSizeLimit: Long
) extends SinkConfig

case class JsonSinkConfigParams(
  outputBaseDir: String = "/dbfs/tmp/listener/",
  outputPartitions: Map[String, String] = Map(
    "date" -> LocalDateTime.now().format(DateTimeFormatter.ISO_DATE),
    "applicationId" -> "spark.app.id"),
  writeBatchSize: Int = 100,
  maxFileSize: Long = 1L*1024*1024
)

object JsonSinkConfig {
  def build(sparkConf: Map[String, String], inputParams: JsonSinkConfigParams): JsonSinkConfig = {
    val outputDir: String = buildPath(sparkConf, inputParams.outputBaseDir, inputParams.outputPartitions)

    val conf = JsonSinkConfig(
      destination = outputDir,
      writeBatchSize = inputParams.writeBatchSize,
      fileSizeLimit = inputParams.maxFileSize
    )
    conf
  }
}

final case class LogSinkConfig() extends SinkConfig