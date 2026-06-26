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

          // Custom stagingDir set via Config (code-first API)
          val customStagingDir = Files.createTempDirectory("custom-staging-")
          try {
            val conf2 = Config(
              destination = s"dbfs:${remoteDir.toString}/",
              stagingDir = customStagingDir.toString + "/"
            )
            val jsonSink2 = new JsonSink(
              config = conf2,
              sparkConf = spark.sparkContext.getConf,
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

            val conf3 = Config(
              destination = s"dbfs:${remoteDir.toString}/",
              stagingDir = nestedStagingDir.getAbsolutePath + "/"
            )
            val jsonSink3 = new JsonSink(
              config = conf3,
              sparkConf = spark.sparkContext.getConf,
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

  describe("JsonSink HDFS mode end-to-end with lazy init (simulates spark.extraListeners)") {

    it("should lazily initialize HadoopFilePromoter and promote files to remote destination") {
      val remoteDir = Files.createTempDirectory("remote-lazy-init-")
      val stagingDir = Files.createTempDirectory("staging-lazy-")

      try {
        // Simulate the lazy init path: HadoopFilePromoter constructed with a thunk
        // that is NOT evaluated at construction time
        var thunkCalled = false
        val hadoopConf = new org.apache.hadoop.conf.Configuration()
        hadoopConf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)

        val promoter = new HadoopFilePromoter(
          remoteDir.toUri.toString,
          () => { thunkCalled = true; hadoopConf }
        )

        // Thunk should NOT have been called at construction
        thunkCalled shouldBe false

        // Write a file to staging dir and promote it — this triggers lazy init
        val localFile = new File(stagingDir.toFile, "test-reports-abc123.json")
        Files.write(localFile.toPath, """{"id":1}""".getBytes)

        promoter.promote(localFile)

        // Now the thunk should have been called
        thunkCalled shouldBe true

        // File should be at remote destination
        val remoteFile = new File(remoteDir.toFile, "test-reports-abc123.json")
        remoteFile.exists() shouldBe true
        remoteFile.length() should be > 0L

        // Local file should be deleted (delSrc=true)
        localFile.exists() shouldBe false
      } finally {
        new Directory(remoteDir.toFile).deleteRecursively()
        new Directory(stagingDir.toFile).deleteRecursively()
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
