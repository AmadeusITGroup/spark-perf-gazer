package org.apache.spark // to access TaskMetrics

import org.apache.spark.executor.{InputMetrics, OutputMetrics, ShuffleReadMetrics, ShuffleWriteMetrics, TaskMetrics}
import org.apache.spark.scheduler.StageInfo
import org.apache.spark.storage.RDDInfo
import org.scalamock.scalatest.MockFactory

object Fixtures2 {

  object Stage1 extends MockFactory {

    case class TaskMetrics2() extends TaskMetrics {
      override val shuffleReadMetrics = new ShuffleReadMetrics2()
      override val shuffleWriteMetrics = new ShuffleWriteMetrics2()
      override val outputMetrics = new OutputMetrics2()
      override val inputMetrics = new InputMetrics2()
    }
    class ShuffleReadMetrics2() extends ShuffleReadMetrics {override val totalBytesRead: Long = 1024 * 1024 * 75}
    class ShuffleWriteMetrics2() extends ShuffleWriteMetrics {override val bytesWritten: Long = 1024 * 1024 * 76}
    class OutputMetrics2() extends OutputMetrics {override val bytesWritten: Long = 1024 * 1024 * 74}
    class InputMetrics2() extends InputMetrics {override val bytesRead: Long = 1024 * 1024 * 5}

    val taskMetrics: TaskMetrics = {
      val tm = mock[TaskMetrics2]
      (tm.memoryBytesSpilled _).expects().returns(1024 * 1024 * 3).anyNumberOfTimes()
      (tm.diskBytesSpilled _).expects().returns(1024 * 1024 * 4).anyNumberOfTimes()
      (tm.executorCpuTime _).expects().returns(1000L * 1000 * 1000 * 77).anyNumberOfTimes()
      (tm.executorRunTime _).expects().returns(1000L * 1000 * 1000 * 98).anyNumberOfTimes()
      (tm.jvmGCTime _).expects().returns(1000L * 1000 * 1000 * 13).anyNumberOfTimes()
      tm
    }

    val stageInfo = new StageInfo(
      stageId = 1,
      name = "aname",
      numTasks = 2,
      rddInfos = Seq.empty[RDDInfo],
      parentIds = Seq.empty[Int],
      details = "somedetails",
      resourceProfileId = 1,
      attemptId = 8,
      taskMetrics = Fixtures2.Stage1.taskMetrics
    )
  }

}
