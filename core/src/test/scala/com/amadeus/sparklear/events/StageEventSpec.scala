package com.amadeus.sparklear.events

import com.amadeus.testfwk.SimpleSpec
import org.apache.spark.Fixtures2
import org.scalamock.scalatest.MockFactory

class StageEventSpec extends SimpleSpec with MockFactory {
  describe(s"The ${StageEvent.getClass.getSimpleName}") {
    it("should generate correct metrics") {
      val rs = StageEvent(Fixtures2.Stage1.stageInfo)
      rs.execCpuNs shouldEqual (77L*1000*1000*1000)
      rs.execRunNs shouldEqual (98L*1000*1000*1000)
      rs.execjvmGCNs shouldEqual (13L*1000*1000*1000)
      rs.attempt shouldEqual 8
      rs.spillBytes shouldEqual 3*1024*1024
      rs.inputReadBytes shouldEqual 5L*1024*1024
      rs.outputWriteBytes shouldEqual 74L*1024*1024
      rs.shuffleReadBytes shouldEqual 75L*1024*1024
      rs.shuffleWriteBytes shouldEqual 76L*1024*1024
    }
  }
}
