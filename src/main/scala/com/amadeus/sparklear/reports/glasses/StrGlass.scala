package com.amadeus.sparklear.reports.glasses

import com.amadeus.sparklear.reports.{Report, StrReport}

case class StrGlass(
  strRegex: Option[String] = None
) extends Glass {
  override def eligible(r: Report): Boolean = r match {
    case i: StrReport => strRegex.map(r => i.s.matches(r)).getOrElse(true)
    case _ => true
  }
}
