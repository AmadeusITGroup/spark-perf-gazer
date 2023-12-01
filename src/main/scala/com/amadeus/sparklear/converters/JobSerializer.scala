package com.amadeus.sparklear.converters

import com.amadeus.sparklear.Config
import com.amadeus.sparklear.input.JobInput
import com.amadeus.sparklear.output.OutputString
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{write => asJson}

sealed trait JobSerializer extends Serializer[JobInput, OutputString]

case object JobJson extends JobSerializer {
  override def toOutput(c: Config, r: JobInput): Seq[OutputString] = {
    Seq(OutputString(asJson(r)(DefaultFormats)))
  }
}

case object JobPretty extends JobSerializer {
  override def toOutput(c: Config, r: JobInput): Seq[OutputString] = {
    val w = r.w
    val v = w.endUpdate.map { u =>
      val totalExecCpuTimeSec = u.finalStages.collect { case (_, Some(stgStats)) => stgStats.execCpuSecs }.sum
      val spillMb = u.finalStages.collect { case (_, Some(stgStats)) => stgStats.spillMb }.flatten.sum
      val spillReport = if (spillMb != 0) s"SPILL_MB=$spillMb" else ""
      val header =
        s"JOB ID=${u.jobEnd.jobId} GROUP='${w.group}' NAME='${w.name}' SQL_ID=${w.sqlId} ${spillReport}"
      val jobStats = s"STAGES=${w.initialStages.size} TOTAL_CPU_SEC=${totalExecCpuTimeSec}"
      s"$header $jobStats"
    }
    v.map(OutputString.apply).toSeq
  }
}
