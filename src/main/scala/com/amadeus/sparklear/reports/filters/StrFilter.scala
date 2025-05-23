package com.amadeus.sparklear.reports.filters

import com.amadeus.sparklear.reports.{Report, StrReport}

case class StrFilter(
  strRegex: Option[String] = None
) extends Filter {
  override def eligible(r: Report): Boolean = r match {
    case i: StrReport => strRegex.map(r => i.s.matches(r)).getOrElse(true)
    case _ => true
  }
}
