package com.amadeus.perfgazer

import org.scalatest.matchers.should.Matchers
import com.amadeus.testfwk.SimpleSpec
import com.amadeus.testfwk.TempDirSupport

class JsonSinkViewDDLGeneratorSpec extends SimpleSpec with Matchers {

  describe("JsonSink.JsonViewDDLGenerator.generateViewDDL") {
    it("should handle a simple path with no ending /") {
      val path = "/tmp"
      val ddl = JsonSink.JsonViewDDLGenerator.generateViewDDL(path, "sql")
      ddl should include ("path \"/tmp/sql-reports-*.json\"")
    }

    it("should handle a simple path with intermediate / and no partitions") {
      val path = "/tmp/listener"
      val ddl = JsonSink.JsonViewDDLGenerator.generateViewDDL(path, "sql")
      ddl should include ("path \"/tmp/listener/sql-reports-*.json\"")
    }

    it("should handle a path with one partition segment") {
      val path = "/tmp/listener/date=2025-09-10"
      val ddl = JsonSink.JsonViewDDLGenerator.generateViewDDL(path, "sql")
      ddl should include ("path \"/tmp/listener/date=2025-09-10/sql-reports-*.json\"")
    }

    it("should handle a path with multiple partition segments") {
      val path = "/tmp/listener/date=2025-09-10/cluster=111/id=ffff/level=ggg"
      val ddl = JsonSink.JsonViewDDLGenerator.generateViewDDL(path, "sql")
      ddl should include ("path \"/tmp/listener/date=2025-09-10/cluster=111/id=ffff/level=ggg/sql-reports-*.json\"")
    }

    it("should handle a path with only partition segments after base") {
      val path = "/base/a=10/b=20/c=30"
      val ddl = JsonSink.JsonViewDDLGenerator.generateViewDDL(path, "sql")
      ddl should include ("path \"/base/a=10/b=20/c=30/sql-reports-*.json\"")
    }

    it("should handle a path with non-partition segments between partitions") {
      val path = "/base/a=10/something/b=10/c=30"
      val ddl = JsonSink.JsonViewDDLGenerator.generateViewDDL(path, "sql")
      ddl should include ("path \"/base/a=10/something/b=10/c=30/sql-reports-*.json\"")
    }

    it("should handle a path with no partitions and a different report name") {
      val path = "/tmp/listener"
      val ddl = JsonSink.JsonViewDDLGenerator.generateViewDDL(path, "job")
      ddl should include ("path \"/tmp/listener/job-reports-*.json\"")
    }

    it("should handle a path with no leading slash") {
      val path = "dbfs:/tmp/listener/date=2025-09-10"
      val ddl = JsonSink.JsonViewDDLGenerator.generateViewDDL(path, "sql")
      ddl should include ("path \"dbfs:/tmp/listener/date=2025-09-10/sql-reports-*.json\"")
    }

    it("should handle a path with trailing slash in directory") {
      val path = "/tmp/listener/date=2025-09-10/"
      val ddl = JsonSink.JsonViewDDLGenerator.generateViewDDL(path, "sql")
      ddl should include ("path \"/tmp/listener/date=2025-09-10/sql-reports-*.json\"")
    }

    it("should handle a path with backslashes (windows style)") {
      val path = "C:\\tmp\\listener\\date=2025-09-10"
      val ddl = JsonSink.JsonViewDDLGenerator.generateViewDDL(path, "sql")
      ddl should include ("path \"C:\\tmp\\listener\\date=2025-09-10\\sql-reports-*.json\"")
    }

    it("should handle a path starting with /dbfs mountpoint, replacing it with dbfs: if on Databricks") {
      val databricksGenerator = new JsonSink.JsonViewDDLGenerator {
        override protected def runningOnDatabricks: Boolean = true
      }
      val path = "/dbfs/tmp/listener/date=2025-09-10"
      val ddl = databricksGenerator.generateViewDDL(path, "sql")
      ddl should include ("path \"dbfs:/tmp/listener/date=2025-09-10/sql-reports-*.json\"")
    }

    it("should show the full set of snippets generated for a typical resolved destination") {
      TempDirSupport.withTmpDir{tmp =>
        val fixedUuid = "00000000-0000-0000-0000-000000000042"
        val fixedNow = java.time.LocalDateTime.of(2025, 4, 1, 0, 0)
        val destination = f"$tmp/pg/{{perfgazer.now.year}}-{{perfgazer.now.month}}/runId={{perfgazer.runid}}"
        val sparkConf = new org.apache.spark.SparkConf(false)
          .set(JsonSink.DestinationKey, destination)
        val sink = new JsonSink(
          JsonSink.Config(destination = destination),
          sparkConf,
          uuidGen = () => java.util.UUID.fromString(fixedUuid),
          nowProvider = () => fixedNow
        )
        val snippets = sink.generateAllViewSnippets()

        snippets should have size 4
        snippets should contain (
          s"""|CREATE OR REPLACE TEMPORARY VIEW sql
            |USING json
            |OPTIONS (
            |  path "$tmp/pg/2025-04/runId=00000000-0000-0000-0000-000000000042/sql-reports-*.json"
            |);""".stripMargin
        )
        snippets should contain (
          s"""|CREATE OR REPLACE TEMPORARY VIEW job
            |USING json
            |OPTIONS (
            |  path "$tmp/pg/2025-04/runId=00000000-0000-0000-0000-000000000042/job-reports-*.json"
            |);""".stripMargin
        )
        snippets should contain (
          s"""|CREATE OR REPLACE TEMPORARY VIEW stage
            |USING json
            |OPTIONS (
            |  path "$tmp/pg/2025-04/runId=00000000-0000-0000-0000-000000000042/stage-reports-*.json"
            |);""".stripMargin
        )
        snippets should contain (
          s"""|CREATE OR REPLACE TEMPORARY VIEW task
            |USING json
            |OPTIONS (
            |  path "$tmp/pg/2025-04/runId=00000000-0000-0000-0000-000000000042/task-reports-*.json"
            |);""".stripMargin
        )
      }
    }

  }
}
