package com.amadeus.integration

import com.amadeus.testfwk.SimpleSpec
import com.amadeus.testfwk.SparkSupport.{DefaultConfigs, withSpark}
import com.amadeus.testfwk.TempDirSupport.withTmpDir
import com.jayway.jsonpath.JsonPath
import org.apache.spark.SparkConf
import org.apache.spark.scheduler.PerfGazerReplay

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.collection.JavaConverters._

class ReplayEventLogSpec extends SimpleSpec {

  describe("PerfGazer replay on Spark event logs") {
    it("should write SQL reports with scan metrics from replayed event logs") {
      withTmpDir { tmpDir =>
        val eventLogDir = tmpDir.resolve("eventlog")
        Files.createDirectories(eventLogDir)
        val outputDir = tmpDir.resolve("perfgazer-out")
        Files.createDirectories(outputDir)
        val csvDir = tmpDir.resolve("csv")
        Files.createDirectories(csvDir)
        val csvPath = csvDir.resolve("data.csv")
        Files.write(
          csvPath,
          "id,name\n1,a\n2,b\n".getBytes(StandardCharsets.UTF_8)
        )

        val conf = DefaultConfigs ++ List(
          ("spark.eventLog.enabled", "true"),
          ("spark.eventLog.dir", eventLogDir.toUri.toString),
          ("spark.eventLog.rolling.enabled", "false")
        )

        withSpark(conf, appName = this.getClass.getName) { spark =>
          val df = spark.read.option("header", "true").csv(csvPath.toString)
          df.count()
        }

        val logPath = findEventLogPath(eventLogDir)

        val replayConf = new SparkConf(false)
          .set("spark.perfgazer.sink.class", "com.amadeus.perfgazer.JsonSink")
          .set("spark.perfgazer.sink.json.destination", outputDir.toString)
          .set("spark.perfgazer.sql.enabled", "true")
          .set("spark.perfgazer.jobs.enabled", "false")
          .set("spark.perfgazer.stages.enabled", "false")
          .set("spark.perfgazer.tasks.enabled", "false")

        PerfGazerReplay.replay(Seq(logPath.toString), replayConf, Some(false))

        val sqlReportFiles = findReportFiles(outputDir, prefix = "sql-reports-")
        printFiles("sql report", sqlReportFiles)

        val sqlReports = readNonEmptyLines(sqlReportFiles)
        sqlReports should have size 2

        sqlReports.foreach { report =>
          jsonString(report, "$.details") should include("== Physical Plan ==")
        }

        val allNodes = sqlReports.flatMap(readNodes)

        val scanTextNode = allNodes.find(nodeName(_).contains("Scan text")).getOrElse {
          fail(s"No Scan text node found in replayed SQL reports:\n${sqlReports.mkString("\n")}")
        }
        metric(scanTextNode, "number of files read") shouldBe "1"
        metric(scanTextNode, "number of output rows") shouldBe "1"
        isLeaf(scanTextNode) shouldBe true

        val scanCsvNode = allNodes.find(nodeName(_).contains("Scan csv")).getOrElse {
          fail(s"No Scan csv node found in replayed SQL reports:\n${sqlReports.mkString("\n")}")
        }
        metric(scanCsvNode, "number of files read") shouldBe "1"
        metric(scanCsvNode, "number of output rows") shouldBe "2"
        isLeaf(scanCsvNode) shouldBe true
      }
    }
  }

  private def findEventLogPath(eventLogDir: Path): Path = {
    val entries = listFiles(eventLogDir)
      .filterNot(p => p.getFileName.toString.startsWith("."))
      .filterNot(p => p.getFileName.toString.endsWith(".crc"))
      .filterNot(p => p.getFileName.toString.endsWith(".inprogress"))
    entries.headOption.getOrElse {
      throw new IllegalStateException(s"No Spark event log found in $eventLogDir")
    }
  }

  private def findReportFiles(outputDir: Path, prefix: String): Seq[Path] =
    listFiles(outputDir)
      .filter(p => p.getFileName.toString.startsWith(prefix))
      .filter(p => p.getFileName.toString.endsWith(".json"))

  private def listFiles(dir: Path): Seq[Path] = {
    val stream = Files.list(dir)
    try {
      stream.iterator().asScala.toSeq.sortBy(_.getFileName.toString)
    } finally {
      stream.close()
    }
  }

  private def readUtf8(path: Path): String =
    new String(Files.readAllBytes(path), StandardCharsets.UTF_8)

  private def readNonEmptyLines(paths: Seq[Path]): Seq[String] =
    paths.flatMap(path => readUtf8(path).split("\\R").map(_.trim).filter(_.nonEmpty))

  private def jsonString(json: String, path: String): String =
    JsonPath.parse(json).read[String](path)

  private def readNodes(json: String): Seq[java.util.Map[String, AnyRef]] =
    JsonPath.parse(json).read[java.util.List[java.util.Map[String, AnyRef]]]("$.nodes").asScala.toSeq

  private def nodeName(node: java.util.Map[String, AnyRef]): String =
    node.get("nodeName").toString

  private def isLeaf(node: java.util.Map[String, AnyRef]): Boolean =
    node.get("isLeaf").asInstanceOf[Boolean]

  private def metric(node: java.util.Map[String, AnyRef], key: String): String = {
    val metrics = node.get("metrics").asInstanceOf[java.util.Map[String, AnyRef]].asScala
    metrics.get(key).map(_.toString).getOrElse {
      throw new IllegalStateException(s"Metric '$key' not found in node ${nodeName(node)}: $metrics")
    }
  }

  private def printFiles(label: String, paths: Seq[Path]): Unit = {
    println(s"[ReplayEventLogSpec] ${label}s written to ${paths.headOption.map(_.getParent).getOrElse("<none>")}")
    if (paths.isEmpty) {
      println(s"[ReplayEventLogSpec] no ${label} files found")
    } else {
      paths.foreach { path =>
        println(s"[ReplayEventLogSpec] ===== ${path.getFileName} =====")
        println(readUtf8(path))
      }
    }
  }
}
