package com.amadeus.perfgazer

import com.amadeus.perfgazer.JsonSink.{Config, getClass}
import com.amadeus.perfgazer.BufferedReportWriter.FilePrintWriter
import org.json4s.jackson.Serialization
import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization.{write => asJson}
import org.slf4j.{Logger, LoggerFactory}

import java.io.{File, FileWriter, PrintWriter}
import java.util.UUID
import scala.collection.mutable.ListBuffer
import com.amadeus.perfgazer.reports.{Report, ReportType}

object BufferedReportWriter {
  private class FilePrintWriter(val file: File, val writer: PrintWriter)
}

/**
 * This class is not thread-safe.
 */
class BufferedReportWriter(config: Config, reportType: ReportType, dir: String) {
  val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  private val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)

  // mutable variables
  private var currentFile = newFilePrintWriter()
  private val buffer: ListBuffer[Report] = new ListBuffer[Report]()

  def write(report: Report): Unit = {
    logger.trace("Adding report to buffer '{}'...", reportType)
    buffer += report
    if (buffer.size >= config.writeBatchSize) {
      logger.trace("Buffer '{}' reached writeBatchSize threshold (file '{}')", reportType, currentFile.file: Any)
      flush()
    }
  }

  def flush(): Unit = {
    if (buffer.nonEmpty) {
      logger.trace("Flushing reports from buffer '{}' to file '{}'", reportType: Any, currentFile.file: Any)
      buffer.foreach(r => currentFile.writer.println(asJson(r)(formats))) // scalastyle:ignore regex
      // flush writer to write to disk
      currentFile.writer.flush()
      // clear reports
      buffer.clear()
    }
    if (currentFile.file.length() >= config.fileSizeLimit) {
      switchToNewRollingFile()
    }
  }

  def close(): Unit = {
    flush()
    currentFile.writer.close()
    logger.trace("Closed buffer '{}'", reportType)
  }

  private def switchToNewRollingFile(): Unit = {
    logger.trace("Rolling file {} has reached the fileSizeLimit threshold ({} bytes)...",
      currentFile.file.getPath, currentFile.file.length())
    currentFile.writer.close()
    currentFile = newFilePrintWriter()
    logger.trace("Switched to new rolling file {}.", currentFile.file.getPath)
  }

  private def newFilePrintWriter(): FilePrintWriter = {
    val folder = new File(dir)
    if (!folder.exists()) folder.mkdirs()
    val path = s"$dir/${reportType.name}-reports-${UUID.randomUUID()}.json"
    val file = new File(path)
    val writer = new PrintWriter(new FileWriter(file, true))
    new FilePrintWriter(file, writer)
  }

}
