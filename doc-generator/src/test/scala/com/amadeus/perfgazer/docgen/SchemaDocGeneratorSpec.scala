package com.amadeus.perfgazer.docgen

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.Files

class SchemaDocGeneratorSpec extends AnyFunSpec with Matchers {

  /** Run the generator into a temp directory and return (mdContent, jsonContent). */
  private def generate(): (String, String) = {
    val tmpDir = Files.createTempDirectory("docgen-test").toFile
    val mdPath = new File(tmpDir, "data_model.md").getAbsolutePath
    val jsonPath = new File(tmpDir, "schema.json").getAbsolutePath
    try {
      SchemaDocGenerator.main(Array(mdPath, jsonPath))
      val md = scala.io.Source.fromFile(mdPath).mkString
      val json = scala.io.Source.fromFile(jsonPath).mkString
      (md, json)
    } finally {
      new File(mdPath).delete()
      new File(jsonPath).delete()
      tmpDir.delete()
    }
  }

  describe("SchemaDocGenerator") {

    lazy val (md, json) = generate()

    // ── Markdown output ───────────────────────────────────────────────

    describe("Markdown output") {

      it("should contain the page title") {
        md should include("# Data Model Reference")
      }

      it("should document all four report views") {
        md should include("## `job` view")
        md should include("## `sql` view")
        md should include("## `stage` view")
        md should include("## `task` view")
      }

      it("should include the table header row") {
        md should include("| Column | SQL Type | Unit | Description |")
      }

      it("should render field names from JobReport") {
        md should include("| jobId |")
        md should include("| jobStartTime |")
      }

      it("should render SQL types for primitive fields") {
        // jobId is Long -> BIGINT
        md should include("`BIGINT`")
        // stages is Seq[Int] -> ARRAY<INT>
        md should include("`ARRAY<INT>`")
      }

      it("should render units when present") {
        // jobStartTime has unit = "ms"
        md should include("ms")
      }

      it("should render nested type sub-sections for SqlReport") {
        md should include("### `SqlNode`")
      }
    }

    // ── JSON output ───────────────────────────────────────────────────

    describe("JSON output") {

      it("should be valid JSON with project metadata") {
        json should include("\"project\": \"PerfGazer\"")
      }

      it("should contain all four views") {
        json should include("\"name\": \"job\"")
        json should include("\"name\": \"sql\"")
        json should include("\"name\": \"stage\"")
        json should include("\"name\": \"task\"")
      }

      it("should include field types") {
        json should include("\"type\": \"BIGINT\"")
        json should include("\"type\": \"STRING\"")
      }

      it("should include units where specified") {
        json should include("\"unit\": \"ms\"")
      }

      it("should include nested schemas for complex types") {
        json should include("\"nestedSchema\"")
      }

      it("should include field descriptions") {
        json should include("\"description\": \"Unique job identifier\"")
      }
    }

    // ── File generation ───────────────────────────────────────────────

    describe("file generation") {

      it("should create output files at the specified paths") {
        val tmpDir = Files.createTempDirectory("docgen-file-test").toFile
        val mdFile = new File(tmpDir, "sub/data_model.md")
        val jsonFile = new File(tmpDir, "sub/schema.json")
        try {
          SchemaDocGenerator.main(Array(mdFile.getAbsolutePath, jsonFile.getAbsolutePath))
          mdFile.exists() shouldBe true
          jsonFile.exists() shouldBe true
          mdFile.length() should be > 0L
          jsonFile.length() should be > 0L
        } finally {
          mdFile.delete()
          jsonFile.delete()
          new File(tmpDir, "sub").delete()
          tmpDir.delete()
        }
      }
    }
  }
}
