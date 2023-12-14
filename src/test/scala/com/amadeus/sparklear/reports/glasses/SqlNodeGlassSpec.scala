package com.amadeus.sparklear.reports.glasses

import com.amadeus.sparklear.reports.{Report, SqlPlanNodeReport}
import com.amadeus.sparklear.translators.Translator.{EntityName, StringReport}
import com.amadeus.testfwk.SimpleSpec

class SqlNodeGlassSpec extends SimpleSpec {
  case object OtherReport extends Report {
    override def entity: EntityName = "ENTITY"
    override def asStringReport: StringReport = "SR"
  }
  describe(s"The ${SqlNodeGlass.getClass.getSimpleName}") {

    val r = SqlPlanNodeReport(1, "jobName", "nodeName", "0.0", Map("m1" -> "v1"), false, "parent")

    it(s"should be able to tell eligibility on ${SqlPlanNodeReport.getClass.getSimpleName} job name") {

      SqlNodeGlass(jobNameRegex = Some(".*XXX.*")).eligible(r) shouldBe false
      SqlNodeGlass(jobNameRegex = Some(".*obName.*")).eligible(r) shouldBe true
      SqlNodeGlass(jobNameRegex = Some(".*obName.*")).eligible(OtherReport) shouldBe true
    }

    it(s"should be able to tell eligibility on ${SqlPlanNodeReport.getClass.getSimpleName} node name") {
      SqlNodeGlass(nodeNameRegex = Some(".*XXX.*")).eligible(r) shouldBe false
      SqlNodeGlass(nodeNameRegex = Some(".*deName.*")).eligible(r) shouldBe true
      SqlNodeGlass(nodeNameRegex = Some(".*deName.*")).eligible(OtherReport) shouldBe true
    }

    it(s"should be able to tell eligibility on ${SqlPlanNodeReport.getClass.getSimpleName} parent node name") {
      SqlNodeGlass(parentNodeNameRegex = Some(".*XXX.*")).eligible(r) shouldBe false
      SqlNodeGlass(parentNodeNameRegex = Some(".*rent.*")).eligible(r) shouldBe true
      SqlNodeGlass(parentNodeNameRegex = Some(".*rent.*")).eligible(OtherReport) shouldBe true
    }

    it(s"should be able to tell eligibility on ${SqlPlanNodeReport.getClass.getSimpleName} metric name") {
      SqlNodeGlass(metricRegex = Some(".*XXX.*")).eligible(r) shouldBe false
      SqlNodeGlass(metricRegex = Some(".*m.*")).eligible(r) shouldBe true
      SqlNodeGlass(metricRegex = Some(".*m.*")).eligible(OtherReport) shouldBe true
    }

    it(s"should be able to tell eligibility on ${SqlPlanNodeReport.getClass.getSimpleName} leaf type") {
      SqlNodeGlass(isLeaf = Some(true)).eligible(r) shouldBe false
      SqlNodeGlass(isLeaf = Some(false)).eligible(r) shouldBe true
      SqlNodeGlass(isLeaf = Some(false)).eligible(OtherReport) shouldBe true
    }
  }
}
