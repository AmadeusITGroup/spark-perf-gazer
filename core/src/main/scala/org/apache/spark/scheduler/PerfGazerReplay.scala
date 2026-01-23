package org.apache.spark.scheduler

import com.amadeus.perfgazer.PerfGazer
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.history.EventLogFileReader
import org.apache.spark.util.Utils
import org.slf4j.LoggerFactory

object PerfGazerReplay {
  private val logger = LoggerFactory.getLogger(getClass.getName)

  def replay(
    logPaths: Seq[String],
    sparkConf: SparkConf,
    maybeTruncated: Option[Boolean] = None
  ): Unit = {
    if (logPaths.isEmpty) {
      throw new IllegalArgumentException("At least one event log path is required.")
    }

    val listener = new PerfGazer(sparkConf)
    val replayBus = new ReplayListenerBus()
    replayBus.addListener(listener)

    val hadoopConf = SparkHadoopUtil.get.newConfiguration(sparkConf)

    try {
      logPaths.foreach { pathStr =>
        val path = new Path(pathStr)
        val fs = path.getFileSystem(hadoopConf)
        val reader = EventLogFileReader(fs, path).getOrElse {
          throw new IllegalArgumentException(s"Path is not a Spark event log: $pathStr")
        }
        val shouldMaybeTruncate = maybeTruncated.getOrElse(!reader.completed)
        logger.info("Replaying Spark event log: {}", reader.rootPath)
        parseEventLogs(reader.listEventLogFiles, fs, replayBus, shouldMaybeTruncate)
      }
    } finally {
      listener.close()
    }
  }

  private def parseEventLogs(
    logFiles: Seq[FileStatus],
    fs: FileSystem,
    replayBus: ReplayListenerBus,
    maybeTruncated: Boolean
  ): Unit = {
    var continueReplay = true
    logFiles.foreach { file =>
      if (continueReplay) {
        Utils.tryWithResource(EventLogFileReader.openEventLog(file.getPath, fs)) { in =>
          continueReplay = replayBus.replay(in, file.getPath.toString, maybeTruncated)
        }
      }
    }
  }
}
