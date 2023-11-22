package com.amadeus.sparklear.reports

import com.amadeus.sparklear.Config
import com.amadeus.sparklear.reports.Report.StringReport

/**
  * Output report useful for the end-user
  */
trait Report {
  def toStringReport(c: Config): StringReport
}

object Report {
  type StringReport = String
}
