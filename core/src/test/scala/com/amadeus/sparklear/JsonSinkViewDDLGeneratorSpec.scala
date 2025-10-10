package com.amadeus.sparklear

import org.scalatest.matchers.should.Matchers
import com.amadeus.testfwk.SimpleSpec

class JsonSinkViewDDLGeneratorSpec extends SimpleSpec with Matchers {
  describe("JsonSink.JsonViewDDLGenerator.generateViewDDL") {
    it("should handle a simple path with no partitions") {
      val path = "/tmp/listener"
      val ddl = JsonSink.JsonViewDDLGenerator.generateViewDDL(path, "sql")
      ddl should include ("path \"/tmp/listener/sql-reports-*.json\"")
      ddl should include ("basePath \"/tmp/listener/\"")
    }

    it("should handle a path with one partition segment") {
      val path = "/tmp/listener/date=2025-09-10"
      val ddl = JsonSink.JsonViewDDLGenerator.generateViewDDL(path, "sql")
      ddl should include ("path \"/tmp/listener/*/sql-reports-*.json\"")
      ddl should include ("basePath \"/tmp/listener/\"")
    }

    it("should handle a path with multiple partition segments") {
      val path = "/tmp/listener/date=2025-09-10/cluster=111/id=ffff/level=ggg"
      val ddl = JsonSink.JsonViewDDLGenerator.generateViewDDL(path, "sql")
      ddl should include ("path \"/tmp/listener/*/*/*/*/sql-reports-*.json\"")
      ddl should include ("basePath \"/tmp/listener/\"")
    }

    it("should handle a path with non-partition segments between partitions") {
      val path = "/base/a=10/something/b=10/c=30"
      val ddl = JsonSink.JsonViewDDLGenerator.generateViewDDL(path, "sql")
      ddl should include ("path \"/base/a=10/something/*/*/sql-reports-*.json\"")
      ddl should include ("basePath \"/base/a=10/something/\"")
    }

    it("should handle a path with no partitions and a different report name") {
      val path = "/tmp/listener"
      val ddl = JsonSink.JsonViewDDLGenerator.generateViewDDL(path, "job")
      ddl should include ("path \"/tmp/listener/job-reports-*.json\"")
      ddl should include ("basePath \"/tmp/listener/\"")
    }

    it("should handle a path with only partition segments after base") {
      val path = "/base/a=10/b=20/c=30"
      val ddl = JsonSink.JsonViewDDLGenerator.generateViewDDL(path, "sql")
      ddl should include ("path \"/base/*/*/*/sql-reports-*.json\"")
      ddl should include ("basePath \"/base/\"")
    }

    it("should handle a path with no leading slash") {
      val path = "tmp/listener/date=2025-09-10"
      val ddl = JsonSink.JsonViewDDLGenerator.generateViewDDL(path, "sql")
      ddl should include ("path \"/tmp/listener/*/sql-reports-*.json\"")
      ddl should include ("basePath \"/tmp/listener/\"")
    }

    it("should handle a path with trailing slash in directory") {
      val path = "/tmp/listener/date=2025-09-10/"
      val ddl = JsonSink.JsonViewDDLGenerator.generateViewDDL(path, "sql")
      ddl should include ("path \"/tmp/listener/*/sql-reports-*.json\"")
      ddl should include ("basePath \"/tmp/listener/\"")
    }

    it("should handle a path with backslashes (windows style)") {
      val path = "\\tmp\\listener\\date=2025-09-10"
      val ddl = JsonSink.JsonViewDDLGenerator.generateViewDDL(path, "sql")
      ddl should include ("path \"/tmp/listener/*/sql-reports-*.json\"")
      ddl should include ("basePath \"/tmp/listener/\"")
    }
  }
}

