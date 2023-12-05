package com.amadeus.sparklear.reports
import com.amadeus.sparklear.reports.glasses.Glass
import com.amadeus.sparklear.translators.Translator.StringReport

import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{write => asJson}

case class JobReport(
  jobId: Long,
  groupId: String,
  jobName: String,
  sqlId: String,
  spillMb: Long,
  totalExecCpuTimeSec: Long,
  stages: Int
) extends Report {
  override def asStringReport: StringReport = asJson(this)(DefaultFormats)
  override def eligible(g: Glass): Boolean = true
}
