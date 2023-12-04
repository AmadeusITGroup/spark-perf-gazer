package com.amadeus.sparklear.report

import com.amadeus.sparklear.report.glasses.Glass

case class OutputString(s: String) extends Report {
  override def asString(): String = s
  override def eligible(g: Glass): Boolean = true
}
