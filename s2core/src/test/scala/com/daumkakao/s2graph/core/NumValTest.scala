package com.daumkakao.s2graph.core

import org.apache.hadoop.hbase.util.SimplePositionedMutableByteRange
import org.scalatest.{FunSuite, Matchers}

/**
 * Created by shon on 5/24/15.
 */
class NumValTest extends FunSuite with Matchers {
  test("test encode of byte") {
//    val pbr = new SimplePositionedMutableByteRange(Array.fill(10)(0.toByte))
    for {
      testVal <- List(-1.toByte, Int.MinValue, 0.01, 0.04, "abcde")
    } {
      val bytes = DataVal(testVal).bytes.toList
      println(bytes)
    }
  }
}
