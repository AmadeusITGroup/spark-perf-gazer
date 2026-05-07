package com.amadeus.perfgazer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.{Logger, LoggerFactory}
import java.io.File
import java.net.URI

/**
 * FilePromoter for HDFS/cloud mode. Copies completed local staging files
 * to the remote destination via Hadoop FileSystem API.
 *
 * The Hadoop FileSystem is initialized lazily on the first promote() call,
 * allowing safe construction during spark.extraListeners initialization
 * (before SparkContext is fully available).
 *
 * @param destinationDir    The remote destination directory URI (e.g., "s3a://bucket/path/")
 * @param hadoopConfProvider  Thunk that provides the Hadoop Configuration (evaluated lazily)
 */
class HadoopFilePromoter(destinationDir: String, hadoopConfProvider: () => Configuration) extends FilePromoter {
  private val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  /** Secondary constructor for direct Configuration (used in tests and code-based setup) */
  def this(destinationDir: String, hadoopConf: Configuration) = {
    this(destinationDir, () => hadoopConf)
  }

  private lazy val (fs, remoteDestPath): (FileSystem, Path) = {
    val path = new Path(destinationDir)
    val fileSystem = try {
      FileSystem.get(new URI(destinationDir), hadoopConfProvider())
    } catch {
      case e: Exception =>
        throw new IllegalStateException(
          s"Failed to obtain Hadoop FileSystem for destination: $destinationDir", e)
    }
    // Ensure remote directory exists
    if (!fileSystem.exists(path)) {
      fileSystem.mkdirs(path)
    }
    (fileSystem, path)
  }

  override def promote(localFile: File): Unit = {
    val localPath = new Path(localFile.getAbsolutePath)
    val remotePath = new Path(remoteDestPath, localFile.getName)
    try {
      logger.info("Promoting {} to {}", localFile.getAbsolutePath, remotePath: Any)
      fs.copyFromLocalFile(/* delSrc = */ true, /* overwrite = */ true, localPath, remotePath)
      logger.info("Successfully promoted {} to {}", localFile.getName, remotePath: Any)
    } catch {
      case e: Exception =>
        logger.error(s"Failed to promote ${localFile.getAbsolutePath} to $remotePath. " +
          s"Local file retained for recovery.", e)
    }
  }

  override def close(): Unit = {
    // FileSystem instances are cached by Hadoop; do not close shared instance
  }
}
