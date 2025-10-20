package com.amadeus.sparklear

import org.apache.spark.SparkConf

object SparklEarListener {
  def config(sparkConf: SparkConf): SparklearConfig = {
    SparklearConfig.apply(sparkConf)
  }

  def sink(sparkConf: SparkConf): Sink = {
    sparkConf.getAll.foreach(println)
    val sinkClassNameOption = sparkConf.getOption(SparklearSparkConf.SinkClassKey)
    sinkClassNameOption match {
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
  }
}

import SparklEarListener._

class SparklEarListener(sparkConf: SparkConf) extends SparklEar(c = config(sparkConf), sink(sparkConf))
