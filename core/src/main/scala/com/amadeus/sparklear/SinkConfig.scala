package com.amadeus.sparklear

sealed trait SinkConfig

/** Configuration object for JsonSink
  *
  * @param destination Base directory path where JSON files will be written, e.g., "/dbfs/logs/appid=my-app-id/"
  * @param writeBatchSize Number of reports to accumulate before writing to disk
  * @param fileSizeLimit file size to reach before switching to a new file
 */
final case class JsonSinkConfig private (
  destination: String,
  writeBatchSize: Int,
  fileSizeLimit: Long
) extends SinkConfig

object JsonSinkConfig {
  def build(
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
}

final case class LogSinkConfig() extends SinkConfig