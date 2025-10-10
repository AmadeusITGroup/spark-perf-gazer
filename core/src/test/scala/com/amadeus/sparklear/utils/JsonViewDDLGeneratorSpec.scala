package com.amadeus.sparklear.utils

import org.scalatest.matchers.should.Matchers
import com.amadeus.testfwk.SimpleSpec

class JsonViewDDLGeneratorSpec extends SimpleSpec with Matchers {
  describe("JsonViewDDLGenerator.generateViewDDL") {
    it("should handle a simple path with no partitions") {
      val path = "/tmp/listener/sql-reports-1234.json"
      val ddl = JsonViewDDLGenerator.generateViewDDL(path, "sql")
      ddl should include("path \"/tmp/listener/sql-reports-*.json\"")
      ddl should include("basePath \"/tmp/listener/\"")
    }

    it("should handle a path with one partition segment") {
      val path = "/tmp/listener/date=2025-09-10/sql-reports-1234.json"
      val ddl = JsonViewDDLGenerator.generateViewDDL(path, "sql")
      ddl should include("path \"/tmp/listener/*/sql-reports-*.json\"")
      ddl should include("basePath \"/tmp/listener/\"")
    }

    it("should handle a path with multiple partition segments") {
      val path = "/tmp/listener/date=2025-09-10/cluster=111/id=ffff/level=ggg/sql-reports-1234.json"
      val ddl = JsonViewDDLGenerator.generateViewDDL(path, "sql")
      ddl should include("path \"/tmp/listener/*/*/*/*/sql-reports-*.json\"")
      ddl should include("basePath \"/tmp/listener/\"")
    }

    it("should handle a path with non-partition segments between partitions") {
      val path = "/base/a=10/something/b=10/c=30/sql-reports-1234.json"
      val ddl = JsonViewDDLGenerator.generateViewDDL(path, "sql")
      ddl should include("path \"/base/a=10/something/*/*/sql-reports-*.json\"")
      ddl should include("basePath \"/base/a=10/something/\"")
    }

    it("should handle a path with no partitions and a different report name") {
      val path = "/tmp/listener/job-reports-9999.json"
      val ddl = JsonViewDDLGenerator.generateViewDDL(path, "job")
      ddl should include("path \"/tmp/listener/job-reports-*.json\"")
      ddl should include("basePath \"/tmp/listener/\"")
    }

    it("should handle a path with only partition segments after base") {
      val path = "/base/a=10/b=20/c=30/sql-reports-1234.json"
      val ddl = JsonViewDDLGenerator.generateViewDDL(path, "sql")
      ddl should include("path \"/base/*/*/*/sql-reports-*.json\"")
      ddl should include("basePath \"/base/\"")
    }

    it("should handle a path with no leading slash") {
      val path = "tmp/listener/date=2025-09-10/sql-reports-1234.json"
      val ddl = JsonViewDDLGenerator.generateViewDDL(path, "sql")
      ddl should include("path \"/tmp/listener/*/sql-reports-*.json\"")
      ddl should include("basePath \"/tmp/listener/\"")
    }

    it("should handle a path with trailing slash in directory") {
      val path = "/tmp/listener/date=2025-09-10//sql-reports-1234.json/"
      val ddl = JsonViewDDLGenerator.generateViewDDL(path.stripSuffix("/"), "sql")
      ddl should include("path \"/tmp/listener/*/sql-reports-*.json\"")
      ddl should include("basePath \"/tmp/listener/\"")
    }

    it("should handle a path with backslashes (windows style)") {
      val path = "\\tmp\\listener\\date=2025-09-10\\sql-reports-1234.json"
      val ddl = JsonViewDDLGenerator.generateViewDDL(path, "sql")
      ddl should include("path \"/tmp/listener/*/sql-reports-*.json\"")
      ddl should include("basePath \"/tmp/listener/\"")
    }

    it("should throw if file name is not present in path") {
      val path = "/tmp/listener/date=2025-09-10/"
      an[IllegalArgumentException] should be thrownBy {
        val ddl = JsonViewDDLGenerator.generateViewDDL(path, "sql")
        print(ddl)

      }
    }
  }
}
