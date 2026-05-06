package com.amadeus.perfgazer

import java.io.File

/**
 * Responsible for promoting (copying) completed local staging files
 * to their final destination. Implementations are NOT thread-safe
 * and must be called exclusively from the ReportWriter daemon thread.
 */
trait FilePromoter {
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
