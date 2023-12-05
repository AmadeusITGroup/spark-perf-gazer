package com.amadeus.sparklear.reports

case class StrReport(s: String) extends Report {
  override def asStringReport(): String = s
}
