package com.amadeus.sparklear.translators

import com.amadeus.sparklear.Config
import com.amadeus.sparklear.prereports.PreReport
import com.amadeus.sparklear.reports.Report

trait Translator[I <: PreReport, R <: Report] {
  def toAllReports(c: Config, p: I): Seq[R]
  def toReports(c: Config, p: I): Seq[R] = {
    val rep = toAllReports(c, p)
    val frep = if (c.glasses.isEmpty) { // no glasses? return all
      rep
    } else {
      rep.filter(r => c.glasses.exists(g => g.eligible(r))) // glasses? filter
    }
    frep
  }
  def toStringReports(c: Config, p: I): Seq[Translator.StringReport] =
    toReports(c, p).map(l => s"${c.stringReportPrefix}${l.asStringReport}")
}

object Translator {
  type StringReport = String
}
