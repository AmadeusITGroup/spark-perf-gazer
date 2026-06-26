package com.amadeus.perfgazer

import java.net.URI
import java.nio.file.Paths
import scala.util.Try

sealed trait DestinationMode
object DestinationMode {
  case object Posix extends DestinationMode
  case object Hdfs extends DestinationMode

  /**
   * Detect the destination mode from the resolved destination.
   *
   * The destination is first parsed as a URI, then classified by inspecting its scheme:
   *   - no scheme (e.g. "/tmp/out")                         -> POSIX (local java.io)
   *   - single-letter scheme (e.g. "C:/tmp/out")            -> POSIX (Windows drive letter,
   *                                                            not a real URI scheme)
   *   - any other scheme (s3, s3a, abfss, gs, dbfs, hdfs,
   *     file, ...)                                          -> HDFS (Hadoop FileSystem)
   *
   * Backslashes are not legal in URIs, so a Windows-style path such as "C:\tmp\out"
   * fails URI parsing. As a fallback, when the input contains a backslash, it is
   * parsed as a local filesystem path and classified as POSIX. This fallback is
   * deliberately narrow so that typo'd URIs (e.g. "http://exa mple.com") still
   * surface as failures instead of being silently treated as local paths.
   *
   * An empty destination is rejected up front as a misconfiguration.
   *
   * @return Success(mode) if the destination is a valid path or URI,
   *         Failure(...) otherwise.
   */
  def detect(resolvedDestination: String): Try[DestinationMode] =
    Try {
      require(resolvedDestination.nonEmpty, "destination must not be empty")
      new URI(resolvedDestination)
    }
      .map { uri =>
        Option(uri.getScheme) match {
          case None                     => Posix
          case Some(s) if s.length == 1 => Posix // Windows drive letter, not a URI scheme
          case Some(_)                  => Hdfs
        }
      }
      .recoverWith {
        case _ if resolvedDestination.contains('\\') =>
          // Likely a Windows path; URIs don't permit backslashes.
          Try(Paths.get(resolvedDestination)).map(_ => Posix)
      }
}
