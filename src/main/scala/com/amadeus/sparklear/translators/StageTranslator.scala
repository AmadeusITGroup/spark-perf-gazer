package com.amadeus.sparklear.translators

import com.amadeus.sparklear.Config
import com.amadeus.sparklear.prereports.StagePreReport
import com.amadeus.sparklear.reports.{StrReport, StrStageReport}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{write => asJson}

sealed trait StageTranslator extends Translator[StagePreReport, StrReport]

case object StageJson extends StageTranslator {
  override def toAllReports(c: Config, p: StagePreReport): Seq[StrReport] = {
    Seq(StrStageReport(asJson(p)(DefaultFormats)))
  }
}

case object StagePrettyTranslator extends StageTranslator {
  override def toAllReports(c: Config, r: StagePreReport): Seq[StrReport] = {
    val p = r.w
    val spillRep = p.spillMb.map(i => s" SPILL_MB=$i").mkString
    val attemptRep = s""
    val s = s"STAGE ID=${r.w.stageInfo.stageId} READ_MB=${p.inputReadMb} WRITE_MB=${p.outputWriteMb} SHUFFLE_READ_MB=${p.shuffleReadMb} " +
      s"SHUFFLE_WRITE_MB=${p.shuffleWriteMb} EXEC_CPU_SECS=${p.execCpuSecs} ATTEMPT=${p.attempt}$spillRep"
    Seq(StrStageReport(s))
  }
}
