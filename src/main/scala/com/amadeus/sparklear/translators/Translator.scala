package com.amadeus.sparklear.translators

import com.amadeus.sparklear.Config
import com.amadeus.sparklear.prereports.PreReport
import com.amadeus.sparklear.reports.Report
import com.amadeus.sparklear.translators.Translator.TranslatorName

/**
  * Converts a [[PreReport]] into a [[Report]]
  *
  * @tparam P the [[PreReport]] type
  * @tparam R the [[Report]] type
  */
trait Translator[P <: PreReport, R <: Report] {

  /**
    * Human readable name for this translator
    * @return a lower case string with convention <entity><singlekeyword>
    */
  def name: TranslatorName

  /**
    * Convert a [[PreReport]] P into a collection of [[Report]] R
    * @param c the configuration to perform the conversion
    * @param p the [[PreReport]] to convert
    * @return the collection of [[Report]] generated
    */
  def toAllReports(c: Config, p: P): Seq[R]

  /**
    * Same as [[toAllReports()]] but the addition of filters applied to filter [[Report]]s
    */
  def toReports(c: Config, p: P): Seq[R] = {
    val rep = toAllReports(c, p)
    val frep = if (c.filters.isEmpty) { // no filters? return all
      rep
    } else {
      rep.filter(r => c.filters.exists(g => g.eligible(r))) // filters? filter
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
  type TranslatorName = String
  type StringReport = String
  type EntityName = String

  def forName[T <: Translator[_, _]](s: Seq[T])(e: EntityName, n: TranslatorName): T =
    s.filter(t => t.name.equalsIgnoreCase(n)).headOption
      .getOrElse(throw new IllegalArgumentException(s"Invalid translator '${n}' for entity ${e} (expected one of: ${s.map(_.name).mkString(", ")})"))
}
