package com.amadeus.sparklear.reports.filters

import com.amadeus.sparklear.reports.{Report, StrJobReport, StrReport, filters}
import com.amadeus.sparklear.translators.Translator.{EntityName, StringReport}
import com.amadeus.testfwk.SimpleSpec

class StrFilterSpec extends SimpleSpec {
  case object OtherReport extends Report {
    override def entity: EntityName = "ENTITY"
    override def asStringReport: StringReport = "SR"
  }
  describe(s"The ${StrFilter.getClass.getSimpleName}") {
    it(s"should be able to tell eligibility on ${StrJobReport.getClass.getSimpleName}") {
      filters.StrFilter(Some(".*XXX.*")).eligible(StrJobReport("XXXXX")) shouldBe true
      filters.StrFilter(Some(".*XXX.*")).eligible(StrJobReport("XX")) shouldBe false
      filters.StrFilter(None).eligible(StrJobReport("XX")) shouldBe true
    }
    it(s"should be able to tell eligibility on non-${StrJobReport.getClass.getSimpleName} (always true)") {
      filters.StrFilter(None).eligible(OtherReport) shouldBe true
      filters.StrFilter(Some(".*")).eligible(OtherReport) shouldBe true
    }
  }
}
