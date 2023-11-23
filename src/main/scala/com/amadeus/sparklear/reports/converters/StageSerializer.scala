package com.amadeus.sparklear.reports.converters

import com.amadeus.sparklear.reports.StageReport
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{write => asJson}

sealed trait StageSerializer {
  def output(p: StageReport): String
}

case object StageJson extends StageSerializer {
  override def output(p: StageReport): String = {
    asJson(p)(DefaultFormats)
  }
}

case object StagePretty extends StageSerializer {
  override def output(r: StageReport): String = {
    val p = r.w
    val spillRep = p.spillMb.map(i => s"SPILL_MB=$i").mkString
    val attemptRep = if (p.attempt != 0) s"${p.attempt}" else ""
    s"READ_MB=${p.inputReadMb} WRITE_MB=${p.outputWriteMb} SHUFFLE_READ_MB=${p.shuffleReadMb}" +
      s"SHUFFLE_WRITE_MB=${p.shuffleWriteMb} EXEC_CPU_SECS=${p.execCpuSecs} $spillRep $attemptRep"
  }
}
