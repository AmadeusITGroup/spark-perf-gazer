package com.amadeus.sparklear.input.converters

import com.amadeus.sparklear.Config
import com.amadeus.sparklear.input.{Input, Output}
import com.amadeus.sparklear.input.converters.Serializer.StringReport

object Serializer {
  type StringReport = String
  case class OutputString(s: String) extends Output {
    override def asString(): String = s
  }
}

trait Serializer[I <: Input, K <: Output] {
  def toOutput(c: Config, p: I): Seq[K]
  def toStringReport(c: Config, p: I): StringReport = toOutput(c, p).map(_.asString).mkString("\n")
}
