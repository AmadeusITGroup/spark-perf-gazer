package com.amadeus.perfgazer

import java.io.File

/**
 * FilePromoter for POSIX mode. Does nothing — files are already
 * at their final destination.
 */
class NoOpFilePromoter extends FilePromoter {
  override def promote(localFile: File): Unit = ()
  override def close(): Unit = ()
}
