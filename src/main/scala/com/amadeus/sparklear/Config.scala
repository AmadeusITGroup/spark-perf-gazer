package com.amadeus.sparklear

import com.amadeus.sparklear.reports.Report

/** @param sqlEnabled         whether to expose to end-user SQL queries level reports
  * @param jobsEnabled        whether to expose to end-user job level reports
  * @param stagesEnabled      whether to expose to end-user stage level reports
  * @param tasksEnabled       whether to expose to end-user task level reports
  * @param sink               optional method to use as sink for completed [[Report]] instances
  * @param maxCacheSize       maximum amount of elements [[Event]] to keep in memory (per category)
  *                           too large and could cause OOM on the driver, and too small could cause incomplete reports
  *                           generated, so try stay around 200 to 1000 unless you really know what you're doing.
  */
case class Config(
  sqlEnabled: Boolean = true,
  jobsEnabled: Boolean = true,
  stagesEnabled: Boolean = false,
  tasksEnabled: Boolean = false,
  sink: Sink = new LogSink(),
  maxCacheSize: Int
)