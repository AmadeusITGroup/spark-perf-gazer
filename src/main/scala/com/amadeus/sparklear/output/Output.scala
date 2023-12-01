package com.amadeus.sparklear.output

import com.amadeus.sparklear.converters.Serializer.StringReport
import com.amadeus.sparklear.output.glasses.Glass

trait Output {
  def asString: StringReport
  def eligible(g: Glass): Boolean
}
