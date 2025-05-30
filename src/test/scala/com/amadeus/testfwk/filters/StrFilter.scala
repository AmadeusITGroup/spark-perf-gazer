package com.amadeus.testfwk.filters

import com.amadeus.sparklear.reports.StrReport

case class StrFilter(
  strRegex: Option[String] = None
) extends Filter {
  def eligible(i: StrReport): Boolean = strRegex.map(r => i.s.matches(r)).getOrElse(true)
}
