package com.amadeus.sparklear.report

import com.amadeus.sparklear.report.glasses.Glass

case class StrReport(s: String) extends Report {
  override def asStringReport(): String = s
  override def eligible(g: Glass): Boolean = true
}
