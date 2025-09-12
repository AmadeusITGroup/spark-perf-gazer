package com.amadeus.sparklear

import com.amadeus.sparklear.reports.JobReport
import com.amadeus.testfwk.{SimpleSpec, SinkSupport, TempDirSupport}
import org.json4s.jackson.Serialization
import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization.{write => asJson}

import java.io.File
import scala.io.Source
import java.time.Instant

class JsonSinkSpec extends SimpleSpec with TempDirSupport with SinkSupport {
  describe("json sink") {
    implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)

    it("should write job reports with writeBatchSize = 1") {
      withTmpDir { tmpDir =>
        val jsonSink = new JsonSink(
          destination = s"$tmpDir",
          writeBatchSize = 1)

        val jr = JobReport(1, "testgroup", "testjob", Instant.now.getEpochSecond, Instant.now.getEpochSecond + 1000, "1", Seq(1))
        val jsonFile = new File(jsonSink.jobReportsFile)
        jsonFile.length() should equal(0)

        jsonSink.write(jr)
        jsonFile.length() should not equal 0

        jsonSink.close()
        jsonFile.length() should not equal 0

        // check files content
        val jsonFileSource = Source.fromFile(jsonSink.jobReportsFile)
        val jsonFileContent = jsonFileSource.getLines().mkString
        jsonFileSource.close()
        jsonFileContent should equal(List.fill(1)(asJson(jr)).mkString)
      }
    }
    it("should write job reports with writeBatchSize = 5") {
      withTmpDir { tmpDir =>
        val jsonSink = new JsonSink(
          destination = s"$tmpDir",
          writeBatchSize = 5)

        val jr = JobReport(1, "testgroup", "testjob", Instant.now.getEpochSecond, Instant.now.getEpochSecond + 1000, "1", Seq(1))
        val jsonFile = new File(jsonSink.jobReportsFile)

        jsonSink.write(jr)
        jsonFile.length() should equal(0)
        jsonSink.write(jr)
        jsonFile.length() should equal(0)
        jsonSink.write(jr)
        jsonFile.length() should equal(0)
        jsonSink.write(jr)
        jsonFile.length() should equal(0)
        jsonSink.write(jr)
        jsonFile.length() should not equal 0

        // check files content
        val jsonFileSource = Source.fromFile(jsonSink.jobReportsFile)
        val jsonFileContent = jsonFileSource.getLines().mkString
        jsonFileSource.close()
        jsonFileContent should equal(List.fill(5)(asJson(jr)).mkString)
      }
    }
    it("should write job reports when writeBatchSize not reached and sink is flushed") {
      withTmpDir { tmpDir =>
        val jsonSink = new JsonSink(
          destination = s"$tmpDir",
          writeBatchSize = 5)

        val jr = JobReport(1, "testgroup", "testjob", Instant.now.getEpochSecond, Instant.now.getEpochSecond + 1000, "1", Seq(1))
        val jsonFile = new File(jsonSink.jobReportsFile)

        jsonSink.write(jr)
        jsonFile.length() should equal(0)
        jsonSink.write(jr)
        jsonFile.length() should equal(0)
        jsonSink.write(jr)
        jsonFile.length() should equal(0)
        jsonSink.write(jr)
        jsonFile.length() should equal(0)

        // flush with 4 reports in sink
        jsonSink.close()
        jsonFile.length() should not equal 0

        // check files content
        val jsonFileSource = Source.fromFile(jsonSink.jobReportsFile)
        val jsonFileContent = jsonFileSource.getLines().mkString
        jsonFileSource.close()
        jsonFileContent should equal(List.fill(4)(asJson(jr)).mkString)
      }
    }
  }
}
