package com.amadeus.sparklear.utils

import java.util.concurrent.ConcurrentHashMap
import scala.reflect.ClassTag

/** A thread-safe capped map, that drops the lowest keys when the cap is reached.
  *
  * @param cap       maximum number of items in the map
  * @param evictRatio ratio of items to drop when the cap is reached (between 0.5 and 1.0)
  * @tparam K key type (must be ordered)
  * @tparam V value type
  */
class CappedConcurrentHashMap[K: ClassTag, V](cap: Int, evictRatio: Double = 0.5) {
  require(cap > 2, "Capacity must be greater than 2")
  require(evictRatio >= 0.5 && evictRatio <= 1.0, "Drop ratio (% of items to drop if cap reached) must be in [0.5, 1.0]")

  import scala.collection.JavaConverters._

  private val m = new ConcurrentHashMap[K, V](cap)
  def put(k: K, v: V)(implicit cmp: Ordering[K]): V = {
    if (m.size() >= cap) {
      val keys = m.keys().asScala.toArray.sorted
      val kPercentile = keys(Math.ceil(keys.length * evictRatio).toInt)
      m.keySet().removeIf(k => cmp.lt(k, kPercentile))
    }
    m.put(k, v)
  }
  def remove(k: K): V = {
    m.remove(k)
  }
  def get(k: K): V = m.get(k)
  def size: Int = m.size()
  private[utils] def keys: Iterator[K] = m.keys().asScala
}
