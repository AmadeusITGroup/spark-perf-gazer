package com.amadeus.perfgazer

import com.amadeus.perfgazer.reports._
import com.amadeus.testfwk.{SimpleSpec, SinkSupport}
import org.scalatest.matchers.should.Matchers

class SinkSpec extends SimpleSpec with Matchers with SinkSupport {

  describe("Sink.generateAllViewSnippets") {

    it("should produce the same strings as generating snippets one by one") {
      withTestableSink { sink =>
        // Generate all snippets at once
        val allSnippets = sink.generateAllViewSnippets()

        // Generate snippets one by one
        val individualSnippets = ReportType.values.map(reportType => sink.generateViewSnippet(reportType))

        // They should be the same
        allSnippets shouldBe individualSnippets

        // Verify we have snippets for all report types
        allSnippets.size shouldBe ReportType.values.size
      }
    }
  }
}

