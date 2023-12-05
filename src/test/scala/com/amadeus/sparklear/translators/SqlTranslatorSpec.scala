package com.amadeus.sparklear.translators

import com.amadeus.sparklear.Fixtures
import com.amadeus.sparklear.reports.SqlPlanNodeReport
import com.amadeus.testfwk.{ConfigSupport, SimpleSpec}

class SqlReportSpec extends SimpleSpec with ConfigSupport {
  describe(s"The ${SqlPlanNodeTranslator.getClass.getName}") {
    it("should generate reports in a basic scenario") {
      val reportSql = Fixtures.SqlWrapper1.rootSqlReport
      val cfg = defaultTestConfig
      val rs = SqlPlanNodeTranslator.toReports(cfg, reportSql)
      rs shouldEqual (
        List(
          SqlPlanNodeReport(1, "toto", "OverwriteByExpression", "0", List(), false, ""),
          SqlPlanNodeReport(
            1,
            "toto",
            "Scan csv ",
            "0.0",
            List(
              ("number of output rows", "-1"),
              ("number of files read", "1"),
              ("metadata time", "0"),
              ("size of files read", "44662949")
            ),
            true,
            "OverwriteByExpression"
          )
        )
      )
    }
  }
}
