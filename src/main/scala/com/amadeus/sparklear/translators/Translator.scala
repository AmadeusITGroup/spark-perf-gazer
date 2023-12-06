package com.amadeus.sparklear.translators

import com.amadeus.sparklear.Config
import com.amadeus.sparklear.prereports.PreReport
import com.amadeus.sparklear.reports.Report

/**
  * Converts a [[PreReport]] into a [[Report]]
  *
  * @tparam P the [[PreReport]] type
  * @tparam R the [[Report]] type
  */
trait Translator[P <: PreReport, R <: Report] {
  /**
    * Convert a [[PreReport]] P into a collection of [[Report]] R
    * @param c the configuration to perform the conversion
    * @param p the [[PreReport]] to convert
    * @return the collection of [[Report]] generated
    */
  def toAllReports(c: Config, p: P): Seq[R]

  /**
    * Same as [[toAllReports()]] but the addition of glasses applied to filter [[Report]]s
    */
  def toReports(c: Config, p: P): Seq[R] = {
    val rep = toAllReports(c, p)
    val frep = if (c.glasses.isEmpty) { // no glasses? return all
      rep
    } else {
      rep.filter(r => c.glasses.exists(g => g.eligible(r))) // glasses? filter
    }
    frep
  }

  /**
    * Same as [[toReports()]] but [[Report]] are represented as [[Translator.StringReport]]
    */
  def toStringReports(c: Config, p: P): Seq[Translator.StringReport] =
    toReports(c, p).map(l => s"${c.stringReportPrefix}${l.entity} ${l.asStringReport}")
}

object Translator {
  type StringReport = String
  type EntityName = String

  val EntitySql = "SQL"
  val EntityJob = "JOB"
  val EntityStage = "STAGE"
}
