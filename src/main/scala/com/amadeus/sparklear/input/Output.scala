package com.amadeus.sparklear.input

import com.amadeus.sparklear.input.converters.Serializer.StringReport

trait Output {
  def asString: StringReport
}
