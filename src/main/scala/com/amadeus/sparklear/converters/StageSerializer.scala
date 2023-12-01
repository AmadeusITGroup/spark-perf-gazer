package com.amadeus.sparklear.converters

import com.amadeus.sparklear.Config
import com.amadeus.sparklear.input.StageInput
import com.amadeus.sparklear.output.OutputString
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{write => asJson}

sealed trait StageSerializer extends Serializer[StageInput, OutputString]

case object StageJson extends StageSerializer {
  override def toOutput(c: Config, p: StageInput): Seq[OutputString] = {
    Seq(OutputString(asJson(p)(DefaultFormats)))
  }
}

case object StagePretty extends StageSerializer {
  override def toOutput(c: Config, r: StageInput): Seq[OutputString] = {
    val p = r.w
    val spillRep = p.spillMb.map(i => s" SPILL_MB=$i").mkString
    val attemptRep = if (p.attempt != 0) s"${p.attempt}" else ""
    val s = s"STAGE ID=${r.w.stageInfo.stageId} READ_MB=${p.inputReadMb} WRITE_MB=${p.outputWriteMb} SHUFFLE_READ_MB=${p.shuffleReadMb}" +
      s"SHUFFLE_WRITE_MB=${p.shuffleWriteMb} EXEC_CPU_SECS=${p.execCpuSecs} ATTEMPT=${p.attempt}$spillRep"
    Seq(OutputString(s))
  }
}
