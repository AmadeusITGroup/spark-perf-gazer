package com.amadeus.sparklear

import com.amadeus.sparklear.fixtures.Fixtures
import com.amadeus.testfwk.{ConfigSupport, SimpleSpec}
import org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate

class SparklEarSpec extends SimpleSpec with ConfigSupport {
  describe(s"The listener") {
    it("should not fail upon unhandled messages") {
      val c = defaultTestConfig
      val l = new SparklEar(c)
      val e = SparkListenerSQLAdaptiveExecutionUpdate(1, "", Fixtures.SqlWrapper1.planInfo1)
      l.onOtherEvent(e) // no failure
    }
  }

}
