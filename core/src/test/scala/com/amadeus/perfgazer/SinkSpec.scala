package com.amadeus.perfgazer

import com.amadeus.perfgazer.reports._
import com.amadeus.testfwk.SimpleSpec
import com.amadeus.testfwk.SinkSupport.TestableSink
import org.scalatest.matchers.should.Matchers

class SinkSpec extends SimpleSpec with Matchers {

  describe("Sink.generateAllViewSnippets") {

    it("should produce the same strings as generating snippets one by one") {
      val sink = new TestableSink()
      // Generate all snippets at once
      val allSnippets = sink.generateAllViewSnippets()

      // Generate snippets one by one
      val individualSnippets = sink.supportedReportTypes.map(reportType => sink.generateViewSnippet(reportType))

      // They should be the same
      allSnippets shouldBe individualSnippets

      // Verify we have snippets for all report types
      allSnippets.size shouldBe sink.supportedReportTypes.size
    }
  }
}
