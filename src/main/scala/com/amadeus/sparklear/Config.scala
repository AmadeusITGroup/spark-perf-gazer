package com.amadeus.sparklear

import com.amadeus.sparklear.reports.Report

/** @param sqlEnabled         whether to expose to end-user SQL queries level reports
  * @param jobsEnabled        whether to expose to end-user job level reports
  * @param stagesEnabled      whether to expose to end-user stage level reports
  * @param sink         optional method to use as sink for completed [[Report]] instances
  * @param maxCacheSize       maximum amount of elements [[RawEvent]] to keep in memory (per category)
  *                           too large and could cause OOM on the driver, and too small could cause incomplete reports
  *                           generated, so try stay around 200 to 1000 unless you really know what you're doing.
  */
case class Config(
  sqlEnabled: Boolean = true,
  jobsEnabled: Boolean = true,
  stagesEnabled: Boolean = false,
  sink: Sink = NoopSink, // TODO: no default
  maxCacheSize: Int = Config.DefaultCacheSize // TODO: no default, user must be aware of this
) {
  // TODO: use these to avoid collecting (at the source) some objects if won't be used
}

object Config {
  val DefaultCacheSize: Int = 200
}
