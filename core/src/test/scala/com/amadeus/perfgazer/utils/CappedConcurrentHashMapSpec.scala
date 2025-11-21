package com.amadeus.perfgazer.utils

import com.amadeus.testfwk.SimpleSpec

class CappedConcurrentHashMapSpec extends SimpleSpec {
  describe("The CappedConcurrentHashMap") {
    it("should throw IllegalArgumentException if cap <= 2") {
      an[IllegalArgumentException] should be thrownBy {
        new CappedConcurrentHashMap[Int, Int](2)
      }
    }

    it("should throw IllegalArgumentException if evictRatio is < 0.5") {
      an[IllegalArgumentException] should be thrownBy {
        new CappedConcurrentHashMap[Int, Int](3, 0.49)
      }
    }

    it("should throw IllegalArgumentException if evictRatio is > 1.0") {
      an[IllegalArgumentException] should be thrownBy {
        new CappedConcurrentHashMap[Int, Int](3, 1.01)
      }
    }

    it("should put elements") {
      val m = new CappedConcurrentHashMap[Int, Int](3)
      m.size shouldEqual 0
      m.put(1, 10)
      m.keys.toSet shouldEqual Set(1)
      m.put(2, 20)
      m.keys.toSet shouldEqual Set(1, 2)
      m.put(3, 30)
      m.keys.toSet shouldEqual Set(1, 2, 3)
    }
    it("should remove elements") {
      val m = new CappedConcurrentHashMap[Int, Int](3)
      m.size shouldEqual 0
      m.put(1, 10)
      m.keys.toSet shouldEqual Set(1)
      m.put(2, 20)
      m.keys.toSet shouldEqual Set(1, 2)
      m.remove(1) shouldEqual 10
      m.keys.toSet shouldEqual Set(2)
    }
    it("should be limited and in eviction discard the minimum key element") {
      val m = new CappedConcurrentHashMap[Int, Int](3)
      m.size shouldEqual 0

      m.put(1, 10)
      m.put(2, 20)
      m.put(3, 30)
      m.size shouldEqual 3
      m.keys.toSet shouldEqual Set(1, 2, 3)

      m.put(4, 40) // should trigger a drop of 50% of the elements (1 and 2 as oldest)
      m.size shouldEqual 2
      m.keys.toSet shouldEqual Set(3, 4)

      m.put(5, 50)
      m.size shouldEqual 3
      m.keys.toSet shouldEqual Set(3, 4, 5)

      m.put(6, 60) // should trigger a drop of 50% of the elements (3 and 4 as oldest)
      m.size shouldEqual 2
      m.keys.toSet shouldEqual Set(5, 6)
    }
  }
}
