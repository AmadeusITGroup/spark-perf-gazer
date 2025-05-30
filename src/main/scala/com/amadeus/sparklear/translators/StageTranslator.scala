package com.amadeus.sparklear.translators

import com.amadeus.sparklear.Config
import com.amadeus.sparklear.entities.StageEntity
import com.amadeus.sparklear.reports.{StrReport, StrStageReport}
import com.amadeus.sparklear.translators.Translator.{EntityName, TranslatorName}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{write => asJson}

object StageTranslator {
  val EntityNameStage: EntityName = "STAGE"
  val Translators: Seq[StageTranslator] = Seq(StageJsonTranslator, StagePrettyTranslator)
  def forName(s: TranslatorName): StageTranslator = Translator.forName(Translators)(EntityNameStage, s)
}

sealed trait StageTranslator extends Translator[StageEntity, StrReport]

case object StageJsonTranslator extends StageTranslator {
  override def name: TranslatorName = "stagejson"
  override def toAllReports(c: Config, p: StageEntity): Seq[StrReport] = {
    Seq(StrStageReport(asJson(p)(DefaultFormats)))
  }
}

case object StagePrettyTranslator extends StageTranslator {
  override def name: TranslatorName = "stagepretty"
  override def toAllReports(c: Config, r: StageEntity): Seq[StrReport] = {
    val p = r.end
    val spillRep = p.spillMb.map(i => s" SPILL_MB=$i").mkString
    val attemptRep = s""
    val s = s"STAGE ID=${r.end.stageInfo.stageId} READ_MB=${p.inputReadMb} WRITE_MB=${p.outputWriteMb} SHUFFLE_READ_MB=${p.shuffleReadMb} " +
      s"SHUFFLE_WRITE_MB=${p.shuffleWriteMb} EXEC_CPU_SECS=${p.execCpuSecs} EXEC_RUN_SECS=${p.execRunSecs} EXEC_JVM_GC_SECS=${p.execjvmGCSecs} ATTEMPT=${p.attempt}$spillRep"
    Seq(StrStageReport(s))
  }
}
