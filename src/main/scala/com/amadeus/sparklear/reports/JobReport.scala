package com.amadeus.sparklear.reports
import com.amadeus.sparklear.translators.JobTranslator.EntityNameJob
import com.amadeus.sparklear.translators.Translator.EntityName
//import org.json4s.DefaultFormats
//import org.json4s.jackson.Serialization.{write => asJson}

case class JobReport(
  jobId: Long,
  groupId: String,
  jobName: String,
  sqlId: String,
  spillMb: Long,
  totalExecCpuTimeSec: Long,
  stages: Int
) extends Report {
  override def entity: EntityName = EntityNameJob
}
