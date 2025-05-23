package com.amadeus.sparklear.reports.filters

import com.amadeus.sparklear.reports.{Report, SqlPlanNodeReport}
import com.amadeus.sparklear.translators.Translator.{EntityName, StringReport}
import com.amadeus.testfwk.SimpleSpec

class SqlNodeFilterSpec extends SimpleSpec {
  case object OtherReport extends Report {
    override def entity: EntityName = "ENTITY"
    override def asStringReport: StringReport = "SR"
  }
  describe(s"The ${SqlNodeFilter.getClass.getSimpleName}") {

    val r = SqlPlanNodeReport(1, "jobName", "nodeName", "0.0", Map("m1" -> "v1"), false, "parent")

    it(s"should be able to tell eligibility on ${SqlPlanNodeReport.getClass.getSimpleName} job name") {

      SqlNodeFilter(jobNameRegex = Some(".*XXX.*")).eligible(r) shouldBe false
      SqlNodeFilter(jobNameRegex = Some(".*obName.*")).eligible(r) shouldBe true
      SqlNodeFilter(jobNameRegex = Some(".*obName.*")).eligible(OtherReport) shouldBe true
    }

    it(s"should be able to tell eligibility on ${SqlPlanNodeReport.getClass.getSimpleName} node name") {
      SqlNodeFilter(nodeNameRegex = Some(".*XXX.*")).eligible(r) shouldBe false
      SqlNodeFilter(nodeNameRegex = Some(".*deName.*")).eligible(r) shouldBe true
      SqlNodeFilter(nodeNameRegex = Some(".*deName.*")).eligible(OtherReport) shouldBe true
    }

    it(s"should be able to tell eligibility on ${SqlPlanNodeReport.getClass.getSimpleName} parent node name") {
      SqlNodeFilter(parentNodeNameRegex = Some(".*XXX.*")).eligible(r) shouldBe false
      SqlNodeFilter(parentNodeNameRegex = Some(".*rent.*")).eligible(r) shouldBe true
      SqlNodeFilter(parentNodeNameRegex = Some(".*rent.*")).eligible(OtherReport) shouldBe true
    }

    it(s"should be able to tell eligibility on ${SqlPlanNodeReport.getClass.getSimpleName} metric name") {
      SqlNodeFilter(metricRegex = Some(".*XXX.*")).eligible(r) shouldBe false
      SqlNodeFilter(metricRegex = Some(".*m.*")).eligible(r) shouldBe true
      SqlNodeFilter(metricRegex = Some(".*m.*")).eligible(OtherReport) shouldBe true
    }

    it(s"should be able to tell eligibility on ${SqlPlanNodeReport.getClass.getSimpleName} leaf type") {
      SqlNodeFilter(isLeaf = Some(true)).eligible(r) shouldBe false
      SqlNodeFilter(isLeaf = Some(false)).eligible(r) shouldBe true
      SqlNodeFilter(isLeaf = Some(false)).eligible(OtherReport) shouldBe true
    }
  }
}
