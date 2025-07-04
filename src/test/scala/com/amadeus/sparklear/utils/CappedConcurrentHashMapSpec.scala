package com.amadeus.sparklear.utils

import com.amadeus.testfwk.SimpleSpec

class CappedConcurrentHashMapSpec extends SimpleSpec {
  describe("The concurrent and safe map") {
    it("should put elements") {
      val m = new CappedConcurrentHashMap[Int, Int]("b", 3)
      m.size shouldEqual(0)
      m.put(1, 10)
      m.keys.toSet shouldEqual Set(1)
      m.put(2, 20)
      m.keys.toSet shouldEqual Set(1, 2)
      m.put(3, 30)
      m.keys.toSet shouldEqual Set(1, 2, 3)
    }
    it("should remove elements") {
      val m = new CappedConcurrentHashMap[Int, Int]("b", 3)
      m.size shouldEqual(0)
      m.put(1, 10)
      m.keys.toSet shouldEqual Set(1)
      m.put(2, 20)
      m.keys.toSet shouldEqual Set(1, 2)
      m.remove(1) shouldEqual 10
      m.keys.toSet shouldEqual Set(2)
    }
    it("should be limited and in eviction discard the minimum key element") {
      val m = new CappedConcurrentHashMap[Int, Int]("b", 3)
      m.size shouldEqual(0)
      m.put(1, 10)
      m.put(2, 20)
      m.put(3, 30)
      m.size shouldEqual(3)
      m.keys.toSet shouldEqual Set(1, 2, 3)
      m.put(4, 40)
      m.size shouldEqual(3)
      m.keys.toSet shouldEqual Set(2, 3, 4)
      m.put(5, 50)
      m.put(6, 60)
      m.size shouldEqual(3)
      m.keys.toSet shouldEqual Set(4, 5, 6)
    }
  }
}
