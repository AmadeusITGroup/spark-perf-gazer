package com.amadeus.perfgazer

import com.amadeus.perfgazer.JsonSink.Config
import com.amadeus.perfgazer.reports.{Report, ReportType}
import com.amadeus.testfwk.SimpleSpec
import com.amadeus.testfwk.SparkSupport.withSpark
import com.amadeus.testfwk.TempDirSupport.withTmpDir
import org.apache.spark.SparkConf

import java.io.File
import java.nio.file.Files
import scala.collection.mutable.ListBuffer
import scala.reflect.io.Directory

class JsonSinkModeIntegrationSpec extends SimpleSpec {

  case object TestReportType extends ReportType {
    override def name: String = "test"
  }

  case class TestReport(id: Int) extends Report {
    override def reportType: ReportType = TestReportType
  }

  class TrackingFilePromoter extends FilePromoter {
    val promotedFiles: ListBuffer[File] = ListBuffer.empty[File]
    override def promote(localFile: File): Unit = {
      promotedFiles += localFile
    }
    override def close(): Unit = ()
  }

  describe("NoOpFilePromoter") {

    it("should not modify, move, or delete the file on promote()") {
      val tmpDir = Files.createTempDirectory("noop-promoter-test-")
      try {
        val testFile = new File(tmpDir.toFile, "test-report.json")
        Files.write(testFile.toPath, "test-content".getBytes)
        val originalLength = testFile.length()

        new NoOpFilePromoter().promote(testFile)

        testFile.exists() shouldBe true
        testFile.length() shouldBe originalLength
      } finally {
        new Directory(tmpDir.toFile).deleteRecursively()
      }
    }
  }

  describe("JsonSink mode detection at init") {

    it("should set mode to Posix for local filesystem paths") {
      withTmpDir { tmpDir =>
        val conf = Config(destination = s"$tmpDir")
        val jsonSink = new JsonSink(
          config = conf,
          sparkConf = new SparkConf(),
          reportTypes = Set(TestReportType)
        )

        jsonSink.mode shouldBe DestinationMode.Posix
        jsonSink.close()
      }
    }
  }

  describe("JsonSink HDFS mode staging directory") {

    it("should use default staging directory with applicationId, support custom stagingDir, and create staging dir on init") {
      withSpark(
        conf = List(
          ("spark.hadoop.fs.dbfs.impl", classOf[TestDbfsFileSystem].getName),
          ("spark.hadoop.fs.dbfs.impl.disable.cache", "true")
        ),
        appName = "JsonSinkModeIntegrationSpec-hdfs"
      ) { spark =>
        val appId = spark.sparkContext.applicationId
        val remoteDir = Files.createTempDirectory("remote-dest-")

        try {
          // Default staging directory uses applicationId
          val sparkConf1 = spark.sparkContext.getConf
          val conf1 = Config(destination = s"dbfs:${remoteDir.toString}/")
          val jsonSink1 = new JsonSink(
            config = conf1,
            sparkConf = sparkConf1,
            reportTypes = Set(TestReportType)
          )

          jsonSink1.mode shouldBe DestinationMode.Hdfs

          val expectedStagingDir = s"/tmp/perfgazer/$appId/"
          val stagingDirFile = new File(expectedStagingDir)
          stagingDirFile.exists() shouldBe true
          stagingDirFile.isDirectory shouldBe true
          jsonSink1.close()
          new Directory(stagingDirFile).deleteRecursively()

          // Custom stagingDir from SparkConf
          val customStagingDir = Files.createTempDirectory("custom-staging-")
          try {
            val sparkConf2 = spark.sparkContext.getConf
              .clone()
              .set(JsonSink.StagingDirKey, customStagingDir.toString + "/")

            val conf2 = Config(destination = s"dbfs:${remoteDir.toString}/")
            val jsonSink2 = new JsonSink(
              config = conf2,
              sparkConf = sparkConf2,
              reportTypes = Set(TestReportType)
            )

            jsonSink2.write(TestReport(1))
            jsonSink2.close()

            val remoteFiles = remoteDir.toFile.listFiles()
            remoteFiles should not be null
            remoteFiles.filter(_.isFile).length should be >= 1
          } finally {
            new Directory(customStagingDir.toFile).deleteRecursively()
          }

          // Staging directory created on init even if nested
          val baseTmpDir = Files.createTempDirectory("staging-create-test-")
          val nestedStagingDir = new File(baseTmpDir.toFile, "nested/staging/dir")
          try {
            nestedStagingDir.exists() shouldBe false

            val sparkConf3 = spark.sparkContext.getConf
              .clone()
              .set(JsonSink.StagingDirKey, nestedStagingDir.getAbsolutePath + "/")

            val conf3 = Config(destination = s"dbfs:${remoteDir.toString}/")
            val jsonSink3 = new JsonSink(
              config = conf3,
              sparkConf = sparkConf3,
              reportTypes = Set(TestReportType)
            )

            nestedStagingDir.exists() shouldBe true
            nestedStagingDir.isDirectory shouldBe true
            jsonSink3.close()
          } finally {
            new Directory(baseTmpDir.toFile).deleteRecursively()
          }
        } finally {
          new Directory(remoteDir.toFile).deleteRecursively()
        }
      }
    }
  }

  describe("BufferedReportWriter promotion") {

    it("should call promote on close and on each rolled file") {
      val tmpDir = Files.createTempDirectory("close-promotion-test-")
      try {
        val config = Config(
          destination = tmpDir.toString,
          writeBatchSize = 1,
          fileSizeLimit = 50L // small limit to trigger rolling
        )

        val tracker = new TrackingFilePromoter()
        val writer = new BufferedReportWriter(config, TestReportType, tmpDir.toString, tracker)

        for (i <- 1 to 20) {
          writer.write(TestReport(i))
        }

        writer.close()

        // Multiple files promoted (rolled + final)
        tracker.promotedFiles.size should be >= 2
        tracker.promotedFiles.foreach { file =>
          file.getName should startWith("test-reports-")
          file.getName should endWith(".json")
        }
      } finally {
        new Directory(tmpDir.toFile).deleteRecursively()
      }
    }
  }
}
