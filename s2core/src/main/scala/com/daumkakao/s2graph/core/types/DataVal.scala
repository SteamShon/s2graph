package com.daumkakao.s2graph.core

import org.apache.hadoop.hbase.types.{OrderedBlob, OrderedString, OrderedNumeric}
import org.apache.hadoop.hbase.util._
import java.math.BigDecimal
import org.junit.Assert._

import scala.reflect.ClassTag

/**
 * Created by shon on 5/22/15.
 */
object DataVal {
  type Val = DataVal
  val order = Order.DESCENDING

  val LONG = "long"
  val INT = "int"
  val BYTE = "byte"
  val SHORT = "short"
  val BOOLEAN = "boolean"
  val FLOAT = "float"
  val DOUBLE = "double"
  val STRING = "string"
//  val BIGDECIMAL = "bigdecimal"

  val numOfBytesForHash = 2
  val numOfBytesForTs = 8

  val numOfBitsForDir = 2

  /** currntly len is never used. */


  def valueOf(dataType: String)(pbr: PositionedByteRange, offset: Int): DataVal = {
    pbr.setOffset(offset)
    val value = dataType match {
      case BOOLEAN =>
        DataVal(BOOLEAN)(OrderedBytes.decodeInt8(pbr) == 1)
      case BYTE =>
        DataVal(BYTE)(OrderedBytes.decodeInt8(pbr))
      case SHORT =>
        DataVal(SHORT)(OrderedBytes.decodeInt16(pbr))
      case INT =>
        DataVal(INT)(OrderedBytes.decodeInt32(pbr))
      case LONG =>
        DataVal(LONG)(OrderedBytes.decodeInt64(pbr))
      case FLOAT =>
        DataVal(FLOAT)(OrderedBytes.decodeFloat32(pbr))
      case DOUBLE =>
        DataVal(DOUBLE)(OrderedBytes.decodeFloat64(pbr))
      case STRING =>
        DataVal(STRING)(OrderedBytes.decodeString(pbr))
    }
    value
  }
//  def valueOf(dataType: String)(bytes: Array[Byte], offset: Int = 0, len: Option[Int] = None): DataVal = {
//    val pbr = new SimplePositionedByteRange(bytes, offset, len.getOrElse(bytes.length - offset))
//    val value = dataType match {
//      case BOOLEAN =>
//        DataVal(BOOLEAN)(OrderedBytes.decodeInt8(pbr) == 1)
//      case BYTE =>
//        DataVal(BYTE)(OrderedBytes.decodeInt8(pbr))
//      case SHORT =>
//        DataVal(SHORT)(OrderedBytes.decodeInt16(pbr))
//      case INT =>
//        DataVal(INT)(OrderedBytes.decodeInt32(pbr))
//      case LONG =>
//        DataVal(LONG)(OrderedBytes.decodeInt64(pbr))
//      case FLOAT =>
//        DataVal(FLOAT)(OrderedBytes.decodeFloat32(pbr))
//      case DOUBLE =>
//        DataVal(DOUBLE)(OrderedBytes.decodeFloat64(pbr))
//      case STRING =>
//        DataVal(STRING)(OrderedBytes.decodeString(pbr))
//    }
//    value
//  }

//  def numOfBytes(dataType: String)(value: Any) = {
//    dataType match {
//      case BOOLEAN => 1 + 1
//      case SHORT => 2 + 1
//      case INT => 4 + 1
//      case LONG => 8 + 1
//      case FLOAT => 4 + 1
//      case DOUBLE => 8 + 1
//      case STRING => value.asInstanceOf[String].length + 2
//    }
//  }
  def numOfBytes[T <: Any](t: T) = {
    t match {
      case b @ (_: Byte | _: Boolean) => 1 + 1
      case i: Int => 4 + 1
      case l: Long => 8 + 1
      case f: Float => 4 + 1
      case d: Double => 8 + 1
      case s: String => s.length() + 2
//      case b: BigDecimal => ???
    }
  }
  def withBoolean(b: Boolean): DataVal = DataVal(BOOLEAN)(b)
  def withByte(b: Byte): DataVal = DataVal(BYTE)(b)
  def withShort(s: Short): DataVal = DataVal(SHORT)(s)
  def withInt(i: Int): DataVal = DataVal(INT)(i)
  def withLong(l: Long): DataVal = DataVal(LONG)(l)
  def withFloat(f: Float): DataVal = DataVal(FLOAT)(f)
  def withDouble(d: Double): DataVal = DataVal(DOUBLE)(d)
  def withString(s: String): DataVal = DataVal(STRING)(s)
//  def withBigDecimal(b: BigDecimal): DataVal = DataVal(BIGDECIMAL)(b)
}
/** need to be refactor once understand typeTag
  * may be variable length can be used to save space but not sure yet.
  * */
case class DataVal(dataType: String)(value: Any) {
  import DataVal._
  val bytesNum = DataVal.numOfBytes(value)
  lazy val bytes: Array[Byte] = {
    val pbr = new SimplePositionedMutableByteRange(Array.fill(bytesNum)(0.toByte))
    dataType match {
      case BOOLEAN =>
        OrderedBytes.encodeInt8(pbr, if(value.asInstanceOf[Boolean]) 1 else 0, order)
      case BYTE =>
        OrderedBytes.encodeInt8(pbr, value.asInstanceOf[Byte], order)
      case SHORT =>
          OrderedBytes.encodeInt16(pbr, value.asInstanceOf[Short], order)
      case INT =>
          OrderedBytes.encodeInt32(pbr, value.asInstanceOf[Int], order)
      case LONG =>
          OrderedBytes.encodeInt64(pbr, value.asInstanceOf[Long], order)
      case FLOAT =>
        OrderedBytes.encodeFloat32(pbr, value.asInstanceOf[Float], order)
      case DOUBLE =>
        OrderedBytes.encodeFloat64(pbr, value.asInstanceOf[Double], order)
      case STRING =>
        OrderedBytes.encodeString(pbr, value.asInstanceOf[String], order)
//      case BIGDECIMAL =>
//        OrderedBytes.encodeNumeric(pbr, value.asInstanceOf[BigDecimal], order)
    }
    pbr.getBytes()
  }
  /** risky for now */
  def toVal[T] = {
    value.asInstanceOf[T]
  }
  override def toString(): String = value.toString()
}

object DataValWithTs {
  import DataVal._
  def apply(dataType: String)(pbr: PositionedByteRange, offset: Int): DataValWithTs = {
    var pos = offset
    val dataVal = DataVal.valueOf(dataType)(pbr, offset)
    pos += dataVal.bytes.length
    val ts = Bytes.toLong(pbr.getBytes(), pos, pos + numOfBytesForTs)
    DataValWithTs(dataVal, ts)
  }
}
case class DataValWithTs(dataVal: DataVal, ts: Long) {
  def bytes(): Array[Byte] = Bytes.add(dataVal.bytes, Bytes.toBytes(ts))
}