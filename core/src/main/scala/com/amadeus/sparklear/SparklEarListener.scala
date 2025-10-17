package com.amadeus.sparklear

import org.apache.spark.SparkConf

object SparklEarListener {
  def config(sparkConf: SparkConf): SparklearConfig = {
    SparklearConfig(sparkConf)
  }

  def sink(sparkConf: SparkConf): Sink = {
    sparkConf.getAll.foreach(println)
    val sinkClassNameOption = sparkConf.getOption("spark.sparklear.sink.class")
    sinkClassNameOption match {
      case Some(sinkClassName) =>
        // call sink class constructor with sparkConf
        val params = classOf[SparkConf]
        val sinkClass = Class.forName(sinkClassName)
        val constructor = sinkClass.getDeclaredConstructor(params)
        val sink = constructor.newInstance(sparkConf).asInstanceOf[Sink]
        sink
      case None =>
        throw new IllegalArgumentException("sparklear.sink.class is not set")
    }
  }
}

import SparklEarListener._

class SparklEarListener(sparkConf: SparkConf) extends SparklEar(c = config(sparkConf), sink(sparkConf))
