package com.daumkakao.s2graph.core.types

import com.daumkakao.s2graph.core.DataVal
import org.apache.hadoop.hbase.util.{Bytes, SimplePositionedByteRange}
import org.scalatest.{Matchers, FunSuite}

/**
 * Created by shon on 5/26/15.
 */
class VertexTypeTest extends FunSuite with Matchers {

  import VertexType._

  val columnId = 1
  val stringVal = DataVal("string") _
  val intVal = DataVal("int") _
  val longVal = DataVal("long") _
  val floatVal = DataVal("float") _
  val props = Seq(
    (1.toByte, stringVal("abc")),
    (2.toByte, intVal(37)),
    (3.toByte, floatVal(0.01f))
  )

  def equalsTo(x: Array[Byte], y: Array[Byte]) = Bytes.compareTo(x, y) == 0

  def largerThan(x: Array[Byte], y: Array[Byte]) = Bytes.compareTo(x, y) > 0

  def lessThan(x: Array[Byte], y: Array[Byte]) = Bytes.compareTo(x, y) < 0

  def testOrder[T](sortedSet: IndexedSeq[T], dataType: String)(initial: Any) = {
    val vIds = for {
      isEdge <- List(true, false)
      useHash <- List(true, false)
    } yield {
        var prevVertexId = VertexId(columnId, DataVal(dataType)(initial), isEdge = isEdge, useHash = useHash)
        var startVertexId = prevVertexId

        val rets = for {
          id <- sortedSet
        } yield {
            val vId = VertexId(columnId, DataVal(dataType)(id), isEdge = isEdge, useHash = useHash)
            val comp = largerThan(vId.bytes, startVertexId.bytes) &&
              largerThan(vId.bytes, prevVertexId.bytes)
            prevVertexId = vId
            comp
          }
        rets.forall(x => x)
      }

    val rks = for {
      isEdge <- List(false)
      useHash <- List(true)
    } yield {
        var prevVertexId = VertexId(columnId, DataVal(dataType)(initial), isEdge = isEdge, useHash = useHash)
        var prevVertexRowKey = VertexRowKey(prevVertexId)
        var startVertexId = prevVertexId
        var startVertexRowKey = VertexRowKey(startVertexId)

        val rets = for {
          id <- sortedSet
        } yield {
            val vId = VertexId(columnId, DataVal(dataType)(id), isEdge = isEdge, useHash = useHash)
            val rk = VertexRowKey(vId)
            val pbr = new SimplePositionedByteRange(rk.bytes)
            val decodedRowKey = VertexRowKey.apply(dataType)(pbr, 0)
            val comp = largerThan(vId.bytes, startVertexId.bytes) &&
              largerThan(vId.bytes, prevVertexId.bytes)
            largerThan(rk.bytes, prevVertexRowKey.bytes)
            decodedRowKey == rk
            prevVertexId = vId
            prevVertexRowKey = rk
            comp
          }
        rets.forall(x => x)
      }
    (vIds ++ rks).forall(x => x)
  }

  test("encode/decode vertexId") {
    val rets = for {
      (dataType, innerId) <- List(
        ("string", stringVal("abcdedfs")),
        ("string", stringVal("12345")),
        ("int", intVal(123)),
        ("int", intVal(231)),
        ("float", floatVal(12.3f)),
        ("float", floatVal(14.2f))
      )
      isEdge <- List(true, false)
      useHash <- List(true, false)
    } yield {
        val vertexId = VertexId(columnId, innerId, isEdge = isEdge, useHash = useHash)
        val pbr = new SimplePositionedByteRange(vertexId.bytes)
        val decodedId = VertexId(dataType)(pbr, offset = 0, isEdge = isEdge, useHash = useHash)
        //        println(vertexId, vertexId.bytes, decodedId)
        vertexId == decodedId
      }
    rets.forall(x => x)

  }

  test("sort order of integer VertexId") {
    val ranges = (Int.MinValue + 10 until Int.MinValue + 100) ++
      (-256 until 0) ++ (0 until 256) ++ (Int.MaxValue - 100 to Int.MaxValue)
    testOrder(ranges, "int")(Int.MinValue)
  }

  test("sort order of long VertexId") {
    val ranges = (Long.MinValue + 10 to Long.MinValue + 100) ++ (-256L to 0L) ++ (1L to 256L) ++
      (Long.MaxValue - 100 to Long.MaxValue - 10)
    testOrder(ranges, "long")(Long.MinValue)
  }

  test("sort order of string VertexId") {
    val ranges = List("abc", "abd", "acd", "ace")
    testOrder(ranges.toIndexedSeq, "string")("aaa")
  }
}
