package com.amadeus.sparklear.reports

import com.amadeus.sparklear.reports.glasses.Glass

case class StrReport(s: String) extends Report {
  override def asStringReport(): String = s
  override def eligible(g: Glass): Boolean = true
}
