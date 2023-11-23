package com.amadeus.sparklear.reports

import com.amadeus.sparklear.Config
import com.amadeus.sparklear.annotations.Unstable
import com.amadeus.sparklear.reports.Report.StringReport
import com.amadeus.sparklear.wrappers.StageWrapper

@Unstable
case class StageReport(w: StageWrapper) extends Report {
  override def toStringReport(c: Config): StringReport = c.stageSerializer.output(this)
}
