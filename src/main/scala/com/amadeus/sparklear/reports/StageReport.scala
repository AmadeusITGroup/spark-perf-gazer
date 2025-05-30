package com.amadeus.sparklear.reports

import com.amadeus.sparklear.entities.StageEntity
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{write => toJson}

case class StageReport(
  stageId: Int,
  readMb: Long,
  writeMb: Long,
  shuffleReadMb: Long,
  shuffleWriteMb: Long,
  execCpuSecs: Long,
  execRunSecs: Long,
  execJvmGcSecs: Long,
  attempt: Int,
  spillMb: Long
) extends Report {
  override def asJson: Json = toJson(this)(DefaultFormats) // TODO: use a more efficient serialization
}

object StageReport extends Translator[StageEntity, StageReport] {

  def fromEntityToReport(r: StageEntity): StageReport = {
    val p = r.end
    StageReport(
      stageId = r.end.stageInfo.stageId,
      readMb = p.inputReadMb,
      writeMb = p.outputWriteMb,
      shuffleReadMb = p.shuffleReadMb,
      shuffleWriteMb = p.shuffleWriteMb,
      execCpuSecs = p.execCpuSecs,
      execRunSecs = p.execRunSecs,
      execJvmGcSecs = p.execjvmGCSecs,
      attempt = p.attempt,
      spillMb = p.spillMb.getOrElse(0)
    )
  }
}
