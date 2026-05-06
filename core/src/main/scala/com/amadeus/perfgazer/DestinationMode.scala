package com.amadeus.perfgazer

sealed trait DestinationMode
object DestinationMode {
  case object Posix extends DestinationMode
  case object Hdfs extends DestinationMode

  private val recognizedSchemes: Set[String] = Set(
    "s3://", "s3a://", "abfss://", "gs://", "dbfs:/", "hdfs://"
  )

  /**
   * Detect the destination mode from the resolved path.
   * @throws IllegalArgumentException if the scheme is not recognized
   */
  def detect(resolvedPath: String): DestinationMode = {
    if (resolvedPath.startsWith("/")) {
      Posix
    } else if (recognizedSchemes.exists(scheme => resolvedPath.startsWith(scheme))) {
      Hdfs
    } else {
      throw new IllegalArgumentException(
        s"Unsupported destination scheme in path: $resolvedPath. " +
        s"Expected a POSIX path (starting with /) or a recognized URI scheme: ${recognizedSchemes.mkString(", ")}")
    }
  }
}
