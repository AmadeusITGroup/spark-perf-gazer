package com.amadeus.testfwk.filters

case class StrFilter(
  strRegex: Option[String] = None
) extends Filter {
  def eligible(i: String): Boolean = strRegex.map(r => i.matches(r)).getOrElse(true)
}
