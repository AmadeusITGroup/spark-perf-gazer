package com.amadeus.perfgazer

import com.amadeus.testfwk.SimpleSpec
import com.amadeus.perfgazer.PathBuilder._

class JsonSinkViewDDLGeneratorSpec extends SimpleSpec {

  describe("JsonSink.JsonViewDDLGenerator.generateViewDDL") {
    it("generate a standard view") {
      val path = "/tmp/views"
      val ddl = JsonSink.JsonViewDDLGenerator.generateViewDDL(path, "sql")
      ddl should be (
        """CREATE OR REPLACE TEMPORARY VIEW sql
          |USING json
          |OPTIONS (
          |  path "/tmp/views/sql-reports-*.json"
          |);""".stripMargin)
    }

    it("should handle a path /dbfs/... -> dbfs:/... if on Databricks") {
      val databricksGenerator = new JsonSink.JsonViewDDLGenerator {
        override protected def runningOnDatabricks: Boolean = true
      }
      val path = "/dbfs/tmp/listener/date=2025-09-10"
      val ddl = databricksGenerator.generateViewDDL(path, "sql")
      ddl should include ("path \"dbfs:/tmp/listener/date=2025-09-10/sql-reports-*.json\"")
    }

    it("should use the remote destination path in HDFS mode (not staging dir)") {
      val remoteDestination = "s3a://my-bucket/reports/"
      val ddl = JsonSink.JsonViewDDLGenerator.generateViewDDL(remoteDestination, "sql")

      ddl should include(s"${remoteDestination.normalizePath}sql-reports-*.json")
      ddl should not include "/tmp/perfgazer/"
    }
  }
}
