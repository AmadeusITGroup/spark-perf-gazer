package com.amadeus.sparklear.reports

import com.amadeus.sparklear.entities.StageEntity
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{write => toJson}

case class StageReport(
  stageId: Int,
  readBytes: Long,
  writeBytes: Long,
  shuffleReadBytes: Long,
  shuffleWriteBytes: Long,
  execCpuNs: Long,
  execRunNs: Long,
  execJvmGcNs: Long,
  attempt: Int,
  spillBytes: Long
) extends Report {
  override def asJson: Json = toJson(this)(DefaultFormats) // TODO: use a more efficient serialization
}

object StageReport extends Translator[StageEntity, StageReport] {

  def fromEntityToReport(r: StageEntity): StageReport = {
    val p = r.end
    StageReport(
      stageId = r.end.stageInfo.stageId,
      readBytes = p.inputReadBytes,
      writeBytes = p.outputWriteBytes,
      shuffleReadBytes = p.shuffleReadBytes,
      shuffleWriteBytes = p.shuffleWriteBytes,
      execCpuNs = p.execCpuNs,
      execRunNs = p.execRunNs,
      execJvmGcNs = p.execjvmGCNs,
      attempt = p.attempt,
      spillBytes = p.spillBytes
    )
  }
}
