package com.amadeus.perfgazer.utils

import scala.collection.JavaConverters._

/** Compatibility shim for Java <-> Scala collection conversions.
  * This Scala 2.12 version uses `scala.collection.JavaConverters`.
  */
private[utils] object JavaCollectionOps {
  def enumerationToIterator[A](e: java.util.Enumeration[A]): Iterator[A] = e.asScala
}

