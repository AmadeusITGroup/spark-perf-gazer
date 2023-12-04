package com.amadeus.sparklear.translators

import com.amadeus.sparklear.Config
import com.amadeus.sparklear.prereports.PreReport
import com.amadeus.sparklear.reports.Report

trait Translator[I <: PreReport, R <: Report] {
  def toReport(c: Config, p: I): Seq[R]
  def toStringReport(c: Config, p: I): Translator.StringReport =
    toReport(c, p).map(_.asStringReport).mkString("\n")
}

object Translator {
  type StringReport = String
}
