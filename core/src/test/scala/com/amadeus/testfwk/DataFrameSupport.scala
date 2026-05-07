package com.amadeus.testfwk

import org.apache.spark.sql.DataFrame

object DataFrameSupport {

  implicit class DataFrameOps(val df: DataFrame) extends AnyVal {

    /** Collect rows as sequences of String values for the given column names.
      *
      * Usage:
      * {{{
      * df.collectAs("jobName", "filesRead") should contain(("jobjoin", "1"))
      * }}}
      */
    def collectAs(columns: String*): Array[Product] = {
      df.collect().map { r =>
        val values = columns.map(c => r.getAs[String](c))
        values.length match {
          case 1 => Tuple1(values(0))
          case 2 => (values(0), values(1))
          case 3 => (values(0), values(1), values(2))
          case 4 => (values(0), values(1), values(2), values(3))
          case 5 => (values(0), values(1), values(2), values(3), values(4))
          case _ => throw new IllegalArgumentException(s"collectAs supports up to 5 columns, got ${values.length}")
        }
      }
    }
  }
}
