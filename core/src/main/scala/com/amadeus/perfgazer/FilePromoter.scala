package com.amadeus.perfgazer

import java.io.File

/**
 * Responsible for promoting (copying) completed local staging files
 * to their final destination. Implementations are NOT thread-safe.
 * promote() and close() must be called exclusively from the ReportWriter
 * daemon thread; init() is called once at application start from the
 * listener-bus thread, before any promote() call.
 */
trait FilePromoter {
  /**
   * Initialize the promoter, eagerly acquiring any resources it needs
   * (e.g. the remote FileSystem). Called once at application start so that
   * misconfiguration fails fast rather than on the first promote() call.
   * The default implementation does nothing.
   */
  def init(): Unit = ()

  /**
   * Promote a completed local file to the final destination.
   * After successful promotion, the local file is deleted.
   *
   * @param localFile the completed local staging file to promote
   */
  def promote(localFile: File): Unit

  /**
   * Close and release any resources held by this promoter.
   */
  def close(): Unit
}
