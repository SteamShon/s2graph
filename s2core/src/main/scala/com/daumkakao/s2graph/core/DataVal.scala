package com.daumkakao.s2graph.core

import org.apache.hadoop.hbase.types.{OrderedBlob, OrderedString, OrderedNumeric}
import org.apache.hadoop.hbase.util._
import org.junit.Assert._

import scala.reflect.ClassTag

/**
 * Created by shon on 5/22/15.
 */
object DataVal {

  def valueOf(dataType: String, bytes: Array[Byte], offset: Int, len: Int) = {
    val pbr = new SimplePositionedByteRange(bytes, offset, len)
    val value = dataType match {
      case "boolean" =>
        DataVal("boolean", OrderedBytes.decodeInt8(pbr) == 1)
      case "byte" =>
        DataVal("byte", OrderedBytes.decodeInt8(pbr))
      case "short" =>
        DataVal("short", OrderedBytes.decodeInt16(pbr))
      case "int" =>
        DataVal("int", OrderedBytes.decodeInt32(pbr))
      case "long" =>
        DataVal("long", OrderedBytes.decodeInt64(pbr))
      case "float" =>
        DataVal("float", OrderedBytes.decodeFloat32(pbr))
      case "double" =>
        DataVal("double", OrderedBytes.decodeFloat64(pbr))
      case "string" =>
        DataVal("string", OrderedBytes.decodeString(pbr))
    }
    value
  }

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
  val order = Order.DESCENDING
}
/** need to be refactor once understand typeTag */
case class DataVal(dataType: String, value: Any) {
  import DataVal._
  val bytesNum = DataVal.numOfBytes(value)
  def bytes(): Array[Byte] = {
    val pbr = new SimplePositionedMutableByteRange(Array.fill(bytesNum)(0.toByte))
    dataType match {
      case "boolean" =>
        OrderedBytes.encodeInt8(pbr, if(value.asInstanceOf[Boolean]) 1 else 0, order)
      case "byte" =>
        OrderedBytes.encodeInt8(pbr, value.asInstanceOf[Byte], order)
      case "short" =>
//        val v = value.asInstanceOf[Short]
//        if (v >= Byte.MinValue && v <= Byte.MaxValue)  OrderedBytes.encodeInt8(pbr, value.asInstanceOf[Byte], order)
//        else
          OrderedBytes.encodeInt16(pbr, value.asInstanceOf[Short], order)
      case "int" =>
//        val v = value.asInstanceOf[Int]
//        if (v >= Byte.MinValue && v <= Byte.MaxValue)  OrderedBytes.encodeInt8(pbr, value.asInstanceOf[Byte], order)
//        else if (v >= Short.MinValue && v <= Short.MaxValue) OrderedBytes.encodeInt16(pbr, value.asInstanceOf[Short], order)
//        else
          OrderedBytes.encodeInt32(pbr, value.asInstanceOf[Int], order)
      case "long" =>
//        val v = value.asInstanceOf[Long]
//        if (v >= Byte.MinValue && v <= Byte.MaxValue)  OrderedBytes.encodeInt8(pbr, value.asInstanceOf[Byte], order)
//        else if (v >= Short.MinValue && v <= Short.MaxValue) OrderedBytes.encodeInt16(pbr, value.asInstanceOf[Short], order)
//        else if (v >= Int.MinValue && v <= Int.MaxValue) OrderedBytes.encodeInt32(pbr, value.asInstanceOf[Int], order)
//        else
          OrderedBytes.encodeInt64(pbr, value.asInstanceOf[Long], order)
      case "float" =>
        OrderedBytes.encodeFloat32(pbr, value.asInstanceOf[Float], order)
      case "double" =>
        OrderedBytes.encodeFloat64(pbr, value.asInstanceOf[Double], order)
      case "string" =>
        OrderedBytes.encodeString(pbr, value.asInstanceOf[String], order)
    }
    pbr.getBytes()
  }
}
