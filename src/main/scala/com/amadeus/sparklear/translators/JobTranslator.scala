package com.amadeus.sparklear.translators

import com.amadeus.sparklear.Config
import com.amadeus.sparklear.prereports.JobPreReport
import com.amadeus.sparklear.reports.{JobReport, Report, StrJobReport, StrReport}

sealed trait JobTranslator[T <: Report] extends Translator[JobPreReport, T]

case object JobJsonTranslator extends JobTranslator[JobReport] {
  override def toAllReports(c: Config, preReport: JobPreReport): Seq[JobReport] = {
    val col = preReport.collect
    val end = preReport.endUpdate
    val (spillMb, totalExecCpuTimeSec) = end.spillAndCpu
    Seq(
      JobReport(
        jobId = end.jobEnd.jobId,
        groupId = col.group,
        jobName = col.name,
        sqlId = col.sqlId,
        spillMb = spillMb,
        totalExecCpuTimeSec = totalExecCpuTimeSec,
        stages = col.initialStages.size
      )
    )
  }
}

case object JobPrettyTranslator extends JobTranslator[StrReport] {
  override def toAllReports(c: Config, preReport: JobPreReport): Seq[StrReport] = {
    val col = preReport.collect
    val end = preReport.endUpdate
    val (spillMb, totalExecCpuTimeSec) = end.spillAndCpu
    val spillReport = if (spillMb != 0) s"SPILL_MB=$spillMb" else ""
    val header =
      s"JOB ID=${end.jobEnd.jobId} GROUP='${col.group}' NAME='${col.name}' SQL_ID=${col.sqlId} ${spillReport}"
    val jobStats = s"STAGES=${col.initialStages.size} TOTAL_CPU_SEC=${totalExecCpuTimeSec}"
    Seq(StrJobReport(s"$header $jobStats"))
  }
}
