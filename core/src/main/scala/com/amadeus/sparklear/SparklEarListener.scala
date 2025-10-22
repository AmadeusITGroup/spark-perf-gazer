package com.amadeus.sparklear

import org.apache.spark.SparkConf
import org.slf4j.{Logger, LoggerFactory}

object SparklEarListener {
  implicit lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  def config(sparkConf: SparkConf): SparklearConfig = {
    val conf = SparklearConfig(sparkConf)
    logger.warn(s"SparklearConfig: $conf")
    conf
  }

  def sink(sparkConf: SparkConf): Sink = {
    val sinkClassNameOption = sparkConf.getOption(SparklearSparkConf.SinkClassKey)
    val sink = sinkClassNameOption match {
      case Some(sinkClassName) =>
        // call sink class constructor with sparkConf
        val params = classOf[SparkConf]
        val sinkClass = Class.forName(sinkClassName)
        val constructor = sinkClass.getDeclaredConstructor(params)
        val sink = constructor.newInstance(sparkConf).asInstanceOf[Sink]
        sink
      case None =>
        throw new IllegalArgumentException(SparklearSparkConf.SinkClassKey + " is not set")
    }
    logger.warn(s"Sink: ${sink.asString}")
    sink
  }
}

import SparklEarListener._

class SparklEarListener(sparkConf: SparkConf) extends SparklEar(c = config(sparkConf), sink(sparkConf))
