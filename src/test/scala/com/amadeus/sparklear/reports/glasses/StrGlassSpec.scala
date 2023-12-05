package com.amadeus.sparklear.reports.glasses

import com.amadeus.sparklear.reports.{StrReport, glasses}
import com.amadeus.testfwk.SimpleSpec

class StrGlassSpec extends SimpleSpec {
  describe(s"The ${StrGlass.getClass.getSimpleName}") {
    it("should be able to filter based on matches") {
      glasses.StrGlass(Some(".*XXX.*")).eligible(StrReport("XXXXX")) shouldBe true
      glasses.StrGlass(Some(".*XXX.*")).eligible(StrReport("XX")) shouldBe false
      glasses.StrGlass(None).eligible(StrReport("XX")) shouldBe true
    }
  }
}
