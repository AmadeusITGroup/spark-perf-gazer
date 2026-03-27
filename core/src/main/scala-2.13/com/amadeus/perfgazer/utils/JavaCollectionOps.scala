package com.amadeus.perfgazer.utils

import scala.jdk.CollectionConverters._

/** Compatibility shim for Java <-> Scala collection conversions.
  * This Scala 2.13 version uses `scala.jdk.CollectionConverters`.
  */
private[utils] object JavaCollectionOps {
  def enumerationToIterator[A](e: java.util.Enumeration[A]): Iterator[A] = e.asScala
}

