package com.amadeus.perfgazer

import java.net.URI
import scala.util.Try

sealed trait DestinationMode
object DestinationMode {
  case object Posix extends DestinationMode
  case object Hdfs extends DestinationMode

  /**
   * Detect the destination mode from the resolved destination by parsing it as a URI
   * and inspecting its scheme.
   *   - no scheme (e.g. "/tmp/out")  -> local POSIX path (written directly via java.io)
   *   - any scheme (s3, s3a, abfss, gs, dbfs, hdfs, file, ...) -> Hadoop FileSystem
   *
   * @return Success(mode) if the destination is a valid path or URI,
   *         Failure(URISyntaxException) if it cannot be parsed.
   */
  def detect(resolvedDestination: String): Try[DestinationMode] =
    Try(new URI(resolvedDestination)).map { uri =>
      if (uri.getScheme == null) Posix else Hdfs
    }
}
