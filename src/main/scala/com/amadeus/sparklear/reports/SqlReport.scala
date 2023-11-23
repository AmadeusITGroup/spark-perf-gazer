package com.amadeus.sparklear.reports

import com.amadeus.sparklear.Config
import com.amadeus.sparklear.reports.Report.StringReport
import com.amadeus.sparklear.wrappers.SqlWrapper

case class SqlReport(w: SqlWrapper, m: Map[Long, Long]) extends Report {
  override def toStringReport(c: Config): StringReport = {
    s"${c.prefix} ${c.sqlSerializer.output(this)}"
  }
}
