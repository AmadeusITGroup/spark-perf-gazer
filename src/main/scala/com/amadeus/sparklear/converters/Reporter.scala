package com.amadeus.sparklear.converters

import com.amadeus.sparklear.Config
import com.amadeus.sparklear.input.Input
import com.amadeus.sparklear.report.Report

object Reporter {
  type StringReport = String
}

trait Reporter[I <: Input, K <: Report] {
  def toReport(c: Config, p: I): Seq[K]
  def toStringReport(c: Config, p: I): Reporter.StringReport =
    toReport(c, p).map(_.asStringReport).mkString("\n")
}
