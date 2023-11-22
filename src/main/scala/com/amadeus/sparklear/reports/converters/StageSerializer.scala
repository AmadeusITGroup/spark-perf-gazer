package com.amadeus.sparklear.reports.converters

import com.amadeus.sparklear.wrappers.StageWrapper
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{write => asJson}

sealed trait StageSerializer {
  def output(p: StageWrapper): String
}

case object StageJson extends StageSerializer {
  override def output(p: StageWrapper): String = {
    asJson(p)(DefaultFormats)
  }
}

case object StagePretty extends StageSerializer {
  override def output(p: StageWrapper): String = {
    val spillRep = p.spillMb.map(i => s"SPILL_MB=$i").mkString
    val attemptRep = if (p.attempt != 0) s"${p.attempt}" else ""
    s"READ_MB=${p.inputReadMb} WRITE_MB=${p.outputWriteMb} SHUFFLE_READ_MB=${p.shuffleReadMb}" +
      s"SHUFFLE_WRITE_MB=${p.shuffleWriteMb} EXEC_CPU_SECS=${p.execCpuSecs} $spillRep $attemptRep"
  }
}
