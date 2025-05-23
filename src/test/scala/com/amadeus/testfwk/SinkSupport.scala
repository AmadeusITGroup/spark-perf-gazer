package com.amadeus.testfwk

import com.amadeus.sparklear.Config
import com.amadeus.sparklear.entities.{JobEntity, Entity, SqlEntity, StageEntity}
import com.amadeus.sparklear.reports.Report
import com.amadeus.sparklear.translators.Translator.StringReport
import com.amadeus.testfwk.SinkSupport.AllSinks

import scala.collection.mutable.ListBuffer

object SinkSupport {
  class AllSinks(
                  val preReports: ListBuffer[Entity] = new ListBuffer[Entity](),
                  val reports: ListBuffer[Report] = new ListBuffer[Report](),
                  val stringReports: ListBuffer[StringReport] = new ListBuffer[StringReport]()
  ) {
    def sqlPreReports = preReports.collect { case s: SqlEntity => s }
    def jobPreReports = preReports.collect { case s: JobEntity => s }
    def stagePreReports = preReports.collect { case s: StageEntity => s }
  }
}
trait SinkSupport {

  def withAllSinks[T](testCode: AllSinks => T): T = {
    val allSinks = new AllSinks()
    testCode(allSinks)
  }

  implicit class RichConfig(c: Config) {
    def withSinks(
      s: AllSinks,
      echoPreReport: Boolean = false,
      echoStringReport: Boolean = false,
      echoReport: Boolean = false
    ): Config = c.copy(
      preReportSink = Some { i =>
        if (echoPreReport) { println(i) } // scalastyle:ignore regex
        s.preReports.+=(i)
      },
      stringReportSink = Some { i =>
        if (echoStringReport) { println(i) } // scalastyle:ignore regex
        s.stringReports.+=(i)
      },
      reportSink = Some { i =>
        if (echoReport) { println(i) } // scalastyle:ignore regex
        s.reports.+=(i)
      }
    )
  }

}
