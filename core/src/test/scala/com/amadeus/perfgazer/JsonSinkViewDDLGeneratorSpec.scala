package com.amadeus.perfgazer

import org.scalatest.matchers.should.Matchers
import com.amadeus.testfwk.SimpleSpec

class JsonSinkViewDDLGeneratorSpec extends SimpleSpec with Matchers {

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

  }
}
