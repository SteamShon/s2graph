//package com.daumkakao.s2graph.core
//
//import org.apache.hadoop.hbase.util.SimplePositionedMutableByteRange
//import org.scalatest.{FunSuite, Matchers}
//
///**
// * Created by shon on 5/24/15.
// */
//class NumValTest extends FunSuite with Matchers {
//  test("test encode of byte") {
//
//    for {
//      (testVal, dataType) <- List((-1.toByte, "byte"),
//        (Int.MinValue, "int"),
//        (0.01f, "float"),
//        (0.04, "double"),
//        ("abcde", "string"))
//    } {
//      val bytes = DataVal(dataType, testVal).bytes
//      println(bytes.toList)
//      val parsed = DataVal.valueOf(dataType, bytes, 0, bytes.length)
//      println(parsed)
//      println(parsed.bytes.toList)
//    }
//  }
//}
