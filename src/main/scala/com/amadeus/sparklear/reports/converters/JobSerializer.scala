package com.amadeus.sparklear.reports.converters

import com.amadeus.sparklear.wrappers.JobWrapper
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{write => asJson}

sealed trait JobSerializer {
  def output(p: JobWrapper): String
}

case object JobJson extends JobSerializer {
  override def output(p: JobWrapper): String = {
    asJson(p)(DefaultFormats)
  }
}

case object JobPretty extends JobSerializer {
  override def output(p: JobWrapper): String = ???
}
