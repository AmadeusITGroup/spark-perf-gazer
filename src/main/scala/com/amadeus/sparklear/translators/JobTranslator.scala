package com.amadeus.sparklear.translators

import com.amadeus.sparklear.Config
import com.amadeus.sparklear.prereports.JobPreReport
import com.amadeus.sparklear.reports.StrReport
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{write => asJson}

sealed trait JobTranslator extends Translator[JobPreReport, StrReport]

case object JobJson extends JobTranslator {
  override def toReport(c: Config, r: JobPreReport): Seq[StrReport] = {
    Seq(StrReport(asJson(r)(DefaultFormats)))
  }
}

case object JobPretty extends JobTranslator {
  override def toReport(c: Config, ji: JobPreReport): Seq[StrReport] = {
    val w = ji.w
    val v = {
      val u = ji.e
      val totalExecCpuTimeSec = u.finalStages.collect { case (_, Some(stgStats)) => stgStats.execCpuSecs }.sum
      val spillMb = u.finalStages.collect { case (_, Some(stgStats)) => stgStats.spillMb }.flatten.sum
      val spillReport = if (spillMb != 0) s"SPILL_MB=$spillMb" else ""
      val header =
        s"JOB ID=${u.jobEnd.jobId} GROUP='${w.group}' NAME='${w.name}' SQL_ID=${w.sqlId} ${spillReport}"
      val jobStats = s"STAGES=${w.initialStages.size} TOTAL_CPU_SEC=${totalExecCpuTimeSec}"
      s"$header $jobStats"
    }
    Seq(StrReport(v))
  }
}
