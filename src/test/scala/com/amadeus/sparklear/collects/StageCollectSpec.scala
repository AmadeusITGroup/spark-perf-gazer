package com.amadeus.sparklear.collects

import com.amadeus.testfwk.SimpleSpec
import org.apache.spark.Fixtures2
import org.scalamock.scalatest.MockFactory

class StageCollectSpec extends SimpleSpec with MockFactory {
  describe(s"The ${StageCollect.getClass.getSimpleName}") {
    it("should generate correct metrics") {
      val rs = StageCollect(Fixtures2.Stage1.stageInfo)
      rs.execCpuSecs shouldEqual (77)
      rs.attempt shouldEqual (8)
      rs.spillMb shouldEqual (Some(3))
      rs.inputReadMb shouldEqual 5
      rs.outputWriteMb shouldEqual 74
      rs.shuffleReadMb shouldEqual 75
      rs.shuffleWriteMb shouldEqual 76
    }
  }
}
