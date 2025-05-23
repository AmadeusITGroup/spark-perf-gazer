package com.amadeus.sparklear.events

import com.amadeus.testfwk.SimpleSpec
import org.apache.spark.Fixtures2
import org.scalamock.scalatest.MockFactory

class StageEventSpec extends SimpleSpec with MockFactory {
  describe(s"The ${StageEvent.getClass.getSimpleName}") {
    it("should generate correct metrics") {
      val rs = StageEvent(Fixtures2.Stage1.stageInfo)
      rs.execCpuSecs shouldEqual (77)
      rs.execRunSecs shouldEqual (98)
      rs.execjvmGCSecs shouldEqual (13)
      rs.attempt shouldEqual (8)
      rs.spillMb shouldEqual (Some(3))
      rs.inputReadMb shouldEqual 5
      rs.outputWriteMb shouldEqual 74
      rs.shuffleReadMb shouldEqual 75
      rs.shuffleWriteMb shouldEqual 76
    }
  }
}
