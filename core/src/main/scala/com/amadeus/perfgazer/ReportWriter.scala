package com.amadeus.perfgazer

import com.amadeus.perfgazer.JsonSink.Config
import com.amadeus.perfgazer.reports.{Report, ReportType}
import org.slf4j.{Logger, LoggerFactory}
import ReportWriter._

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import scala.util._

class ReportWriter(val config: Config, val reportType: ReportType, val dir: String) {
  private val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  // Queue of messages/requests to be processed asynchronously by the executor thread
  private val messagesQueue: LinkedBlockingQueue[Message] = new LinkedBlockingQueue[Message]()

  // Executor thread processing the queue
  private val executorThread: Thread = startExecutorThread(s"${getClass.getSimpleName}-${reportType}-executor")

  // Indicates if the executor thread should remain active
  private val executorActive = new AtomicBoolean(true)

  // Buffered writer used by the executor thread
  private val bufferedWriter = new BufferedReportWriter(config, reportType, dir)

  // Indicates if the writer has been closed via close() method
  private val writerClosed = new AtomicBoolean(false)

  private def startExecutorThread(threadName: String): Thread = {
    val thread = new Thread(
      () => {
        while (executorActive.get()) {
          val report = Option(messagesQueue.poll(config.asyncFlushTimeoutMillisecs, TimeUnit.MILLISECONDS))
          report match {
            case Some(WriteReportMessage(r)) => // ask to write report
              bufferedWriter.write(r)
            case None => // regularly flush if no report to process
              bufferedWriter.flush()
            case Some(StopMessage) => // close the buffer and exit
              bufferedWriter.close()
              executorActive.set(false)
          }
        }
        logger.trace("Thread '{}' is now inactive", threadName)
      },
      threadName
    )
    thread.setDaemon(true) // JVM can exit if this thread is running
    thread.start()
    thread
  }

  def write(report: Report): Unit = {
    if (!writerClosed.get()) {
      logger.trace("Adding report to queue '{}'...", reportType)
      messagesQueue.add(WriteReportMessage(report))
    }
  }

  def close(): Unit = {
    logger.trace("Closing writer '{}'...", reportType)
    if (writerClosed.compareAndSet(false, true)) {
      logger.trace("Sending stop pill to writer '{}'...", reportType)
      messagesQueue.add(StopMessage)
      logger.trace("Waiting for thread '{}' to finish...", executorThread.getName)
      val join = Try(executorThread.join(config.waitForGracefulCloseMillisecs))
      logger.trace("Joined thread '{}', result: {}", executorThread.getName, join: Any)
      executorActive.set(false) // ensure the executor is marked as inactive
    } else {
      logger.trace("Skipped closing writer '{}' again (already requested)", reportType)
    }
  }

}

object ReportWriter {
  sealed trait Message
  case class WriteReportMessage(r: Report) extends Message
  case object StopMessage extends Message // signal to stop the executor (stop pill)
}
