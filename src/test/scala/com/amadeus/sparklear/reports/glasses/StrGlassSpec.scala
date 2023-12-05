package com.amadeus.sparklear.reports.glasses

import com.amadeus.sparklear.reports.{Report, StrReport, glasses}
import com.amadeus.sparklear.translators.Translator.StringReport
import com.amadeus.testfwk.SimpleSpec

class StrGlassSpec extends SimpleSpec {
  case object OtherReport extends Report {
    override def asStringReport: StringReport = ""
  }
  describe(s"The ${StrGlass.getClass.getSimpleName}") {
    it(s"should be able to tell eligibility on ${StrReport.getClass.getSimpleName}") {
      glasses.StrGlass(Some(".*XXX.*")).eligible(StrReport("XXXXX")) shouldBe true
      glasses.StrGlass(Some(".*XXX.*")).eligible(StrReport("XX")) shouldBe false
      glasses.StrGlass(None).eligible(StrReport("XX")) shouldBe true
    }
    it(s"should be able to tell eligibility on non-${StrReport.getClass.getSimpleName} (always true)") {
      glasses.StrGlass(None).eligible(OtherReport) shouldBe true
      glasses.StrGlass(Some(".*")).eligible(OtherReport) shouldBe true
    }
  }
}
