package com.amadeus.sparklear.translators

import com.amadeus.sparklear.Config
import com.amadeus.sparklear.entities.Entity
import com.amadeus.sparklear.reports.Report
import com.amadeus.sparklear.translators.Translator.TranslatorName

/**
  * Converts a [[Entity]] into a [[Report]]
  *
  * @tparam P the [[Entity]] type
  * @tparam R the [[Report]] type
  */
trait Translator[P <: Entity, R <: Report] {

  /**
    * Human readable name for this translator
    * @return a lower case string with convention <entity><singlekeyword>
    */
  def name: TranslatorName

  /**
    * Convert a [[Entity]] P into a collection of [[Report]] R
    *
    * @param c the configuration to perform the conversion
    * @param p the [[Entity]] to convert
    * @return the collection of [[Report]] generated
    */
  def toAllReports(c: Config, p: P): Seq[R]

  /**
    * Same as [[toAllReports()]] but the addition of filters applied to filter [[Report]]s
    */
  def toReports(c: Config, p: P): Seq[R] = toAllReports(c, p)
}

object Translator {
  type TranslatorName = String
  type EntityName = String

  def forName[T <: Translator[_, _]](s: Seq[T])(e: EntityName, n: TranslatorName): T =
    s.filter(t => t.name.equalsIgnoreCase(n)).headOption
      .getOrElse(throw new IllegalArgumentException(s"Invalid translator '${n}' for entity ${e} (expected one of: ${s.map(_.name).mkString(", ")})"))
}
