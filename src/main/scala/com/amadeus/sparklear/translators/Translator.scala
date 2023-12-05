package com.amadeus.sparklear.translators

import com.amadeus.sparklear.Config
import com.amadeus.sparklear.prereports.PreReport
import com.amadeus.sparklear.reports.Report

trait Translator[I <: PreReport, R <: Report] {
  def toReport(c: Config, p: I): Seq[R]
  def toStringReport(c: Config, p: I): Seq[Translator.StringReport] =
    toReport(c, p).map(l => s"${c.prefix}${l.asStringReport}")
}

object Translator {
  type StringReport = String
}
