package com.amadeus.sparklear.utils

import java.util.concurrent.ConcurrentHashMap

class CappedConcurrentHashMap[K, V](cap: Int) {
  import scala.collection.JavaConverters._
  private val m = new ConcurrentHashMap[K, V](cap)
  def put(k: K, v: V)(implicit cmp: Ordering[K]): V = {
    if (m.size() >= cap) {
      m.remove(m.keys().asScala.min)
    }
    m.put(k, v)
  }
  def remove(k: K): V = m.remove(k)
  def get(k: K): V = m.get(k)
  def size: Int = m.size()
  def keys: Iterator[K] = m.keys().asScala
  def toScalaMap: scala.collection.immutable.Map[K, V] = m.asScala.toMap
}
