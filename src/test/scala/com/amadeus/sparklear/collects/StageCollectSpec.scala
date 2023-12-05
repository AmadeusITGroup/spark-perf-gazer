package org.apache.spark


import com.amadeus.sparklear.collects.StageCollect
import com.amadeus.testfwk.SimpleSpec
import org.apache.spark.executor.{InputMetrics, OutputMetrics, ShuffleReadMetrics, ShuffleWriteMetrics, TaskMetrics}
import org.apache.spark.scheduler.StageInfo
import org.apache.spark.storage.RDDInfo
import org.scalamock.scalatest.MockFactory

class StageCollectSpec extends SimpleSpec with MockFactory {
  case class TaskMetrics2() extends TaskMetrics {
    override val shuffleReadMetrics = new ShuffleReadMetrics2()
    override val shuffleWriteMetrics = new ShuffleWriteMetrics2()
    override val outputMetrics = new OutputMetrics2()
    override val inputMetrics = new InputMetrics2()
  }
  class ShuffleReadMetrics2() extends ShuffleReadMetrics {override val totalBytesRead = 1024 * 1024 * 75}
  class ShuffleWriteMetrics2() extends ShuffleWriteMetrics {override val bytesWritten = 1024 * 1024 * 76}
  class OutputMetrics2() extends OutputMetrics {override val bytesWritten = 1024 * 1024 * 74}
  class InputMetrics2() extends InputMetrics {override val bytesRead = 1024 * 1024 * 5}
  describe(s"The ${StageCollect.getClass.getSimpleName}") {
    it("should generate correct metrics") {
      val tm = mock[TaskMetrics2]
      (tm.memoryBytesSpilled _).expects().returns(1024 * 1024 * 3).anyNumberOfTimes()
      (tm.executorCpuTime _).expects().returns(1024L * 1024 * 1024 * 77).anyNumberOfTimes()

      val si = new StageInfo(
        stageId = 1,
        name = "aname",
        numTasks = 2,
        rddInfos = Seq.empty[RDDInfo],
        parentIds = Seq.empty[Int],
        details = "somedetails",
        resourceProfileId = 1,
        attemptId = 8,
        taskMetrics = tm
      )
      val rs = StageCollect(si)
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
