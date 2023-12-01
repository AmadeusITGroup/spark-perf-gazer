package com.amadeus.sparklear.output

import com.amadeus.sparklear.output.glasses.Glass

case class OutputString(s: String) extends Output {
  override def asString(): String = s
  override def eligible(g: Glass): Boolean = true
}
