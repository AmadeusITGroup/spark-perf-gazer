package com.amadeus.sparklear.utils

import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.ConcurrentHashMap

class CappedConcurrentHashMap[K, V](name: String, cap: Int) {
  lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  import scala.collection.JavaConverters._
  private val m = new ConcurrentHashMap[K, V](cap)
  def put(k: K, v: V)(implicit cmp: Ordering[K]): V = {
    if (m.size() >= cap) {
      logger.error("Max size reached for {}, removing the oldest element", name)
      // TODO add support to report how many removals took place
      m.remove(m.keys().asScala.min)
    }
    m.put(k, v)
  }
  def remove(k: K): V = m.remove(k)
  def get(k: K): V = m.get(k)
  def size: Int = m.size()
  private[utils] def keys: Iterator[K] = m.keys().asScala
}
