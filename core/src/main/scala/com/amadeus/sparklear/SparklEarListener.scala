package com.amadeus.sparklear

import org.apache.spark.SparkConf

object SparklEarListener {
  def config(): SparklearConfig = {
    SparklearConfig()
  }

  //def sink(sparkConf: SparkConf): Sink = {
  def sink(): Sink = {
    new LogSink()
    /*
    val sinkClassName = sparkConf.get("sparklear.sink.class")
    if (sinkClassName != null) {
      // call sink class constructor with sparkConf
      val params = classOf[SparkConf]
      val sinkClass = Class.forName(sinkClassName)
      val constructor = sinkClass.getDeclaredConstructor(params)
      val sink = constructor.newInstance(sparkConf).asInstanceOf[Sink]
      sink
    } else {
      throw new IllegalArgumentException("sparklear.sink.class is not set")
    }
     */
  }
}

import SparklEarListener._

//class SparklEarListener(sparkConf: SparkConf) extends SparklEar(c = config(sparkConf), sink(sparkConf))
class SparklEarListener(sparkConf: SparkConf) extends SparklEar(c = config(), sink())
