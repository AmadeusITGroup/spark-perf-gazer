package com.amadeus.sparklear.converters

import com.amadeus.sparklear.Config
import com.amadeus.sparklear.input.Input
import Serializer.StringReport
import com.amadeus.sparklear.output.Output

object Serializer {
  type StringReport = String
}

trait Serializer[I <: Input, K <: Output] {
  def toOutput(c: Config, p: I): Seq[K]
  def toStringReport(c: Config, p: I): StringReport = toOutput(c, p).map(_.asString).mkString("\n")
}
