package com.daumkakao.s2graph.core.types

import com.daumkakao.s2graph.core.{GraphUtil, DataVal}
import com.daumkakao.s2graph.core.types.LabelWithDirection
import org.apache.hadoop.hbase.util.{SimplePositionedByteRange, Bytes}
import org.scalatest.{Matchers, FunSuite}

/**
 * Created by shon on 5/26/15.
 */
class EdgeTypeTest extends FunSuite with Matchers {
  import EdgeType._
  import VertexType._

  val columnId = 1
  val labelId = 1
  val dir = 0.toByte
  val labelWithDir = LabelWithDirection(labelId, dir)
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
      isEdge <- List(true)
      useHash <- List(true)
      labelOrderSeq <- List(1.toByte)
      isInverted <- List(true, false)
    } yield {
        var prevVertexId = VertexId(columnId, DataVal(dataType)(initial), isEdge = isEdge, useHash = useHash)
        var prevEdgeRowKey = EdgeRowKey(prevVertexId, labelWithDir, labelOrderSeq, isInverted)
        val startEdgeRowKey = prevEdgeRowKey
        val rets = for {
          id <- sortedSet
        } yield {
            val vertexId = VertexId(columnId, DataVal(dataType)(id), isEdge = isEdge, useHash = useHash)
            val edgeRowKey = EdgeRowKey(vertexId, labelWithDir, labelOrderSeq, isInverted)
            val pbr = new SimplePositionedByteRange(edgeRowKey.bytes)
            val decodedEdgeRowKey = EdgeRowKey.apply(dataType)(pbr, 0)
            val comp = largerThan(edgeRowKey.bytes, startEdgeRowKey.bytes) &&
            largerThan(edgeRowKey.bytes, prevEdgeRowKey.bytes) &&
            edgeRowKey == decodedEdgeRowKey

            prevEdgeRowKey = edgeRowKey
            comp
          }
        rets.forall(x => x)
      }
    vIds.forall(x => x)
  }
  test("sort order of integer EdgeRowKey") {
    val ranges = (Int.MinValue + 10 until Int.MinValue + 100) ++
      (-256 until 0) ++ (0 until 256) ++ (Int.MaxValue - 100 to Int.MaxValue)
    testOrder(ranges, "int")(Int.MinValue)
  }

  test("sort order of long EdgeRowKey") {
    val ranges = (Long.MinValue + 10 to Long.MinValue + 100) ++ (-256L to 0L) ++ (1L to 256L) ++
      (Long.MaxValue - 100 to Long.MaxValue - 10)
    testOrder(ranges, "long")(Long.MinValue)
  }
  test("sort order of EdgeQualifier") {
    val smallProps = Seq((1, intVal(10)), (2, intVal(20)))
    val largeProps = Seq((1, intVal(10)), (2, intVal(21)))
    val tgtVertexId = VertexId(columnId, intVal(10000), isEdge = true, useHash = false)
    val small = EdgeQualifier(smallProps, tgtVertexId, GraphUtil.operations("insert"))
    val pbr = new SimplePositionedByteRange(small.bytes)
    val decodedSmall = EdgeQualifier.apply(Seq("int", "int"), "int")(pbr, 0)
    val large = EdgeQualifier(largeProps, tgtVertexId, GraphUtil.operations("insert"))

    small == decodedSmall && largerThan(large.bytes, small.bytes)

  }
}
