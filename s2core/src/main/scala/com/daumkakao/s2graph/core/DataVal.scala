package com.daumkakao.s2graph.core

import org.apache.hadoop.hbase.types.{OrderedBlob, OrderedString, OrderedNumeric}
import org.apache.hadoop.hbase.util._
import org.junit.Assert._

/**
 * Created by shon on 5/22/15.
 */
object DataVal {
//  def valueOf[T](bytes: Array[Byte], offset: Int, len: Int): DataVal[T] = {
//
//  }
  def numOfBytes[T <: Any](t: T) = {
    t match {
      case b @ (_: Byte | _: Boolean) => 1 + 1
      case i: Int => 4 + 1
      case l: Long => 8 + 1
      case f: Float => 4 + 1
      case d: Double => 8 + 1
      case s: String => s.length() + 2
    }
  }
}

case class DataVal[T <: Any](value: T) {
  val bytesNum = DataVal.numOfBytes(value)
  def bytes(): Array[Byte] = {
    val pbr = new SimplePositionedMutableByteRange(Array.fill(bytesNum)(0.toByte))
    value match {
      case bl: Boolean =>
        OrderedBytes.encodeInt8(pbr, if(bl) 1 else 0, Order.DESCENDING)
      case b : Byte =>
        OrderedBytes.encodeInt8(pbr, b, Order.DESCENDING)
      case i: Int =>
        OrderedBytes.encodeInt32(pbr, i, Order.DESCENDING)
      case l: Long =>
        OrderedBytes.encodeInt64(pbr, l, Order.DESCENDING)
      case f: Float =>
        OrderedBytes.encodeFloat32(pbr, f, Order.DESCENDING)
      case d: Double =>
        OrderedBytes.encodeFloat64(pbr, d, Order.DESCENDING)
      case s: String =>
        OrderedBytes.encodeString(pbr, s, Order.DESCENDING)
    }
    pbr.getBytes()
  }
}
