package com.amadeus.sparklear.reports.glasses

import com.amadeus.sparklear.reports.{Report, StrJobReport, StrReport, glasses}
import com.amadeus.sparklear.translators.Translator.{EntityName, StringReport}
import com.amadeus.testfwk.SimpleSpec

class StrGlassSpec extends SimpleSpec {
  case object OtherReport extends Report {
    override def entity: EntityName = "ENTITY"
    override def asStringReport: StringReport = "SR"
  }
  describe(s"The ${StrGlass.getClass.getSimpleName}") {
    it(s"should be able to tell eligibility on ${StrJobReport.getClass.getSimpleName}") {
      glasses.StrGlass(Some(".*XXX.*")).eligible(StrJobReport("XXXXX")) shouldBe true
      glasses.StrGlass(Some(".*XXX.*")).eligible(StrJobReport("XX")) shouldBe false
      glasses.StrGlass(None).eligible(StrJobReport("XX")) shouldBe true
    }
    it(s"should be able to tell eligibility on non-${StrJobReport.getClass.getSimpleName} (always true)") {
      glasses.StrGlass(None).eligible(OtherReport) shouldBe true
      glasses.StrGlass(Some(".*")).eligible(OtherReport) shouldBe true
    }
  }
}
