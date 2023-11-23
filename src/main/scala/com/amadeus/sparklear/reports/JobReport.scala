package com.amadeus.sparklear.reports

import com.amadeus.sparklear.Config
import com.amadeus.sparklear.reports.Report.StringReport
import com.amadeus.sparklear.wrappers.JobWrapper

case class JobReport(w: JobWrapper) extends Report {
  override def toStringReport(c: Config): StringReport = c.jobSerializer.output(this)
}
