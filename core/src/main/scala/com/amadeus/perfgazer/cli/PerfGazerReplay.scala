package com.amadeus.perfgazer.cli

import org.apache.spark.SparkConf

import scala.annotation.tailrec

object PerfGazerReplay {
  private case class Options(
    logPaths: Vector[String] = Vector.empty,
    sparkConf: SparkConf = new SparkConf(true),
    maybeTruncated: Option[Boolean] = None,
    showHelp: Boolean = false
  )

  private val usage =
    """|Usage: spark-submit --class com.amadeus.perfgazer.cli.PerfGazerReplay <perfgazer-jar> [options]
       |
       |Required:
       |  --log <path>            Spark event log file or rolling event log directory
       |
       |Options:
       |  --conf k=v              Spark configuration (repeatable)
       |  --maybe-truncated       Assume logs may be truncated
       |  --no-maybe-truncated    Force full parsing even if the log looks in-progress
       |  -h, --help              Show this message
       |
       |Example:
       |  spark-submit --class com.amadeus.perfgazer.cli.PerfGazerReplay perfgazer.jar \
       |    --log /tmp/eventlog \
       |    --conf spark.perfgazer.sink.class=com.amadeus.perfgazer.JsonSink \
       |    --conf spark.perfgazer.sink.json.destination=/tmp/out
       |""".stripMargin

  def main(args: Array[String]): Unit = {
    val options = try {
      parseArgs(args.toList, Options())
    } catch {
      case e: IllegalArgumentException =>
        System.err.println(e.getMessage)
        System.err.println(usage)
        sys.exit(1)
    }

    if (options.showHelp) {
      println(usage)
      return
    }

    if (options.logPaths.isEmpty) {
      System.err.println("Missing --log argument.")
      System.err.println(usage)
      sys.exit(1)
    }

    org.apache.spark.scheduler.PerfGazerReplay.replay(
      options.logPaths,
      options.sparkConf,
      options.maybeTruncated
    )
  }

  @tailrec
  private def parseArgs(args: List[String], options: Options): Options = args match {
    case Nil =>
      options
    case ("-h" | "--help") :: tail =>
      parseArgs(tail, options.copy(showHelp = true))
    case "--log" :: value :: tail =>
      parseArgs(tail, options.copy(logPaths = options.logPaths :+ value))
    case "--conf" :: value :: tail =>
      val idx = value.indexOf('=')
      if (idx <= 0 || idx == value.length - 1) {
        throw new IllegalArgumentException(s"Invalid --conf value: $value (expected k=v)")
      }
      options.sparkConf.set(value.substring(0, idx), value.substring(idx + 1))
      parseArgs(tail, options)
    case "--maybe-truncated" :: tail =>
      parseArgs(tail, options.copy(maybeTruncated = Some(true)))
    case "--no-maybe-truncated" :: tail =>
      parseArgs(tail, options.copy(maybeTruncated = Some(false)))
    case value :: tail if !value.startsWith("--") =>
      parseArgs(tail, options.copy(logPaths = options.logPaths :+ value))
    case unknown :: _ =>
      throw new IllegalArgumentException(s"Unknown argument: $unknown")
  }
}
