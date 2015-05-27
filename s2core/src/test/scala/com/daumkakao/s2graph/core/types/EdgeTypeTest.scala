package com.daumkakao.s2graph.core.types

import com.daumkakao.s2graph.core.{Edge, GraphUtil, DataVal}
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

  def testOrder(sortedSet: Seq[DataVal]) = {
    val vIds = for {
      isEdge <- List(true)
      useHash <- List(true)
      labelOrderSeq <- List(1.toByte)
      isInverted <- List(true, false)
    } yield {
        var prevVertexId = VertexId(columnId, sortedSet.head, isEdge = isEdge, useHash = useHash)
        var prevEdgeRowKey = EdgeRowKey(prevVertexId, labelWithDir, labelOrderSeq, isInverted)
        val startEdgeRowKey = prevEdgeRowKey
        val rets = for {
          id <- sortedSet.tail
        } yield {
            val vertexId = VertexId(columnId, id, isEdge = isEdge, useHash = useHash)
            val edgeRowKey = EdgeRowKey(vertexId, labelWithDir, labelOrderSeq, isInverted)
            val pbr = new SimplePositionedByteRange(edgeRowKey.bytes)
            val decodedEdgeRowKey = EdgeRowKey.apply(id.dataType)(pbr, 0)

            println(s"$edgeRowKey > $prevEdgeRowKey")
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
  /**
   * for each target vertexId, test if propsSeqs is ordered properly per target vertexId
   * */
  def testOrderQualifier(propsSeq: Seq[Seq[(Int, DataVal)]], tgtVertexIds: Seq[DataVal]) = {
    val rets = for {
      isEdge <- List(true)
      useHash <- List(false)
      (opStr, op) <- GraphUtil.operations
      vid <- tgtVertexIds
    } yield {
        val vertexId = VertexId(columnId, vid, isEdge = isEdge, useHash = useHash)
        var prev = EdgeQualifier(propsSeq.head, vertexId, op)
        val start = prev

        val rets = for {
          r <- propsSeq.tail
        } yield {
            val idxDataTypes = r.map{ case (seq, dataVal) => (seq -> dataVal.dataType) } toMap
            val cur = EdgeQualifier(r, vertexId, op)
            val pbr = new SimplePositionedByteRange(cur.bytes)
            val decoded = EdgeQualifier(idxDataTypes, vid.dataType)(pbr, 0)
            println(s"$cur > $prev")
            val comp = largerThan(cur.bytes, start.bytes) &&
              largerThan(cur.bytes, prev.bytes) &&
              cur == decoded

            prev = cur
            comp
          }
        rets.forall(x => x)
      }
    rets.forall(x => x)
  }
  /** for each props, check if each target vertex id is ordered properly */
  def testOrderQualifierInverted(propsSeq: Seq[Seq[(Int, DataVal)]], tgtVertexIds: Seq[DataVal]) = {
    val rets = for {
      isEdge <- List(true)
      useHash <- List(false)
      (opStr, op) <- GraphUtil.operations
      props <- propsSeq
    } yield {
        val idxDataTypes = props.map{ case (seq, dataVal) => (seq -> dataVal.dataType) } toMap
        val vertexId = VertexId(columnId, tgtVertexIds.head, isEdge = isEdge, useHash = useHash)
        var prev = EdgeQualifier(props, vertexId, op)
        val start = prev

        val rets = for {
          vid <- tgtVertexIds.tail
        } yield {
            val curVertexId = VertexId(columnId, vid, isEdge = isEdge, useHash = useHash)
            val cur = EdgeQualifier(props, curVertexId, op)
            val pbr = new SimplePositionedByteRange(cur.bytes)
            val decoded = EdgeQualifier(idxDataTypes, vid.dataType)(pbr, 0)
            println(s"$cur > $prev")
            val comp = largerThan(cur.bytes, start.bytes) &&
              largerThan(cur.bytes, prev.bytes) &&
              cur == decoded

            prev = cur
            comp
          }
        rets.forall(x => x)
      }
    rets.forall(x => x)
  }

  test("sort order of integer EdgeRowKey") {
    val ranges = (Int.MinValue + 10 until Int.MinValue + 100) ++
      (-256 until 0) ++ (0 until 256) ++ (Int.MaxValue - 100 to Int.MaxValue)
    testOrder(ranges.map(r => intVal(r.toInt)))
  }

  test("sort order of long EdgeRowKey") {
    val ranges = (Long.MinValue + 10 to Long.MinValue + 100) ++ (-256L to 0L) ++ (1L to 256L) ++
      (Long.MaxValue - 100 to Long.MaxValue - 10)
    testOrder(ranges.map(r => longVal(r.toLong)))
  }

  test("sort order of int props with int id EdgeQualifier") {
    val ranges = Seq(
      Seq((1, intVal(10)), (2, intVal(0))),
      Seq((1, intVal(10)), (2, intVal(20))),
      Seq((1, intVal(10)), (2, intVal(21))),
      Seq((1, intVal(11)), (2, intVal(20)))
    )
    val tgtVertexIds = Seq(
      intVal(10000),
      intVal(10001)
    )
    testOrderQualifier(ranges, tgtVertexIds) &&
      testOrderQualifierInverted(ranges, tgtVertexIds)
  }
  test("sort order of int props with long EdgeQualifier") {
    val ranges = Seq(
      Seq((1, intVal(10)), (2, intVal(0))),
      Seq((1, intVal(10)), (2, intVal(20))),
      Seq((1, intVal(10)), (2, intVal(21))),
      Seq((1, intVal(11)), (2, intVal(20)))
    )
    val tgtVertexIds = Seq(
      DataVal("byte")(Byte.MaxValue),
      intVal(Int.MaxValue),
      longVal(-10L),
      longVal(0L),
      longVal(Int.MaxValue.toLong)
    )
    testOrderQualifier(ranges, tgtVertexIds) &&
      testOrderQualifierInverted(ranges, tgtVertexIds)
  }

}
