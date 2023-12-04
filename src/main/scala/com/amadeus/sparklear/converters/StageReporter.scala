package com.amadeus.sparklear.converters

import com.amadeus.sparklear.Config
import com.amadeus.sparklear.input.StageInput
import com.amadeus.sparklear.report.StrReport
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{write => asJson}

sealed trait StageReporter extends Reporter[StageInput, StrReport]

case object StageJson extends StageReporter {
  override def toReport(c: Config, p: StageInput): Seq[StrReport] = {
    Seq(StrReport(asJson(p)(DefaultFormats)))
  }
}

case object StagePretty extends StageReporter {
  override def toReport(c: Config, r: StageInput): Seq[StrReport] = {
    val p = r.w
    val spillRep = p.spillMb.map(i => s" SPILL_MB=$i").mkString
    val attemptRep = s""
    val s = s"STAGE ID=${r.w.stageInfo.stageId} READ_MB=${p.inputReadMb} WRITE_MB=${p.outputWriteMb} SHUFFLE_READ_MB=${p.shuffleReadMb} " +
      s"SHUFFLE_WRITE_MB=${p.shuffleWriteMb} EXEC_CPU_SECS=${p.execCpuSecs} ATTEMPT=${p.attempt}$spillRep"
    Seq(StrReport(s))
  }
}
