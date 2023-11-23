package com.amadeus.sparklear.reports.converters

import com.amadeus.sparklear.reports.JobReport
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{write => asJson}

sealed trait JobSerializer {
  def output(p: JobReport): String
}

case object JobJson extends JobSerializer {
  override def output(r: JobReport): String = {
    asJson(r)(DefaultFormats)
  }
}

case object JobPretty extends JobSerializer {
  override def output(r: JobReport): String = {
    val w = r.w
    w.endUpdate.map { u =>
      val totalExecCpuTimeSec = u.finalStages.collect { case (_, Some(stgStats)) => stgStats.execCpuSecs }.sum
      val spillMb = u.finalStages.collect { case (_, Some(stgStats)) => stgStats.spillMb }.flatten.sum
      val spillReport = if (spillMb != 0) s"SPILL_MB=$spillMb" else ""
      val header =
        s"JOB ID=${u.jobEnd.jobId} GROUP='${w.group}' NAME='${w.name}' SQL_ID=${w.sqlId} ${spillReport}"
      val jobStats = s"STAGES=${w.initialStages.size} TOTAL_CPU_SEC=${totalExecCpuTimeSec}"
      s"$header $jobStats"
    }.getOrElse("")
  }
}
