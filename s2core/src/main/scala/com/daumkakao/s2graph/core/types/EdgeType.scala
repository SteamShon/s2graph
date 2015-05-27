package com.daumkakao.s2graph.core.types

import com.daumkakao.s2graph.core.{DataValWithTs, DataVal}
import com.daumkakao.s2graph.core.models.{HLabelIndex, HLabelMeta}
import org.apache.hadoop.hbase.util.{PositionedByteRange, Bytes}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by shon on 5/26/15.
 */
object EdgeType {
  import VertexType._

  type Val = DataVal
  type ValTs = DataValWithTs

  def labelOrderSeqWithIsInverted(labelOrderSeq: Byte, isInverted: Boolean): Array[Byte] = {
    assert(labelOrderSeq < (1 << 6))
    val byte = labelOrderSeq << 1 | (if (isInverted) 1 else 0)
    Array.fill(1)(byte.toByte)
  }

  def bytesToLabelIndexSeqWithIsInverted(bytes: Array[Byte], offset: Int): (Byte, Boolean) = {
    val byte = bytes(offset)
    val isInverted = if ((byte & 1) != 0) true else false
    val labelOrderSeq = byte >> 1
    (labelOrderSeq.toByte, isInverted)
  }
  /** mutable */
  def propsToBytes(props: Seq[(Int, DataVal)]): Array[Byte] = {
    val len = props.length
    assert(len <= Byte.MaxValue)
    var bytes = Array.fill(1)(len.toByte)
    for ((k, v) <- props) bytes = Bytes.add(bytes, Array.fill(1)(k.toByte), v.bytes)
    bytes
  }
  def bytesToProps(dataTypes: Seq[String])(pbr: PositionedByteRange, offset: Int) = {
    val bytes = pbr.getBytes()
    var pos = offset
    val len = bytes(pos)
    assert(dataTypes.length == len)
    pos += 1
    val kvs = for (i <- (0 until len)) yield {
      val k = bytes(pos)
      val dataType = dataTypes(i)
      pos += 1
      val v = DataVal.valueOf(dataType)(pbr, pos)
      pos += v.bytes.length
      (k.toInt -> v)
    }
    val ret = (kvs, pos)
    ret
  }

  object EdgeRowKey {
    def apply(dataType: String)(pbr: PositionedByteRange, offset: Int): EdgeRowKey = {
      val bytes = pbr.getBytes()
      var pos = offset
      val vertexId = VertexId(dataType)(pbr, pos, isEdge = true, useHash = true)
      pos += vertexId.bytes.length
      val labelWithDir = LabelWithDirection(Bytes.toInt(bytes, pos, 4))
      pos += 4
      val (labelOrderSeq, isInverted) = bytesToLabelIndexSeqWithIsInverted(bytes, pos)
      EdgeRowKey(vertexId, labelWithDir, labelOrderSeq, isInverted)
    }
  }
  case class EdgeRowKey(srcVertexId: VertexId,
                        labelWithDir: LabelWithDirection,
                        labelOrderSeq: Byte,
                        isInverted: Boolean) {

    lazy val innerSrcVertexId = srcVertexId.updateUseHash(true)
    lazy val bytes = Bytes.add(innerSrcVertexId.bytes,
      labelWithDir.bytes,
      labelOrderSeqWithIsInverted(labelOrderSeq, isInverted))
  }

  object EdgeQualifier {
    def apply(idxDataTypes: Seq[String], dataType: String)(pbr: PositionedByteRange, offset: Int): EdgeQualifier = {
      val bytes = pbr.getBytes()
      var pos = offset

      val (props, tgtVertexId) = {
        val (props, endAt) = bytesToProps(idxDataTypes)(pbr, pos)
        val tgtVertexId = VertexId(dataType)(pbr, endAt, isEdge = true, useHash = false)
        pos = endAt + tgtVertexId.bytes.length
        (props, tgtVertexId)
      }
      val op = bytes(pos)
      EdgeQualifier(props, tgtVertexId, op)
    }
  }
  case class EdgeQualifier(props: Seq[(Int, DataVal)], tgtVertexId: VertexId, op: Byte) {
    val opBytes = Array.fill(1)(op)
    val innerTgtVertexId = tgtVertexId.updateUseHash(false)
    lazy val propsBytes = propsToBytes(props)
    lazy val bytes = Bytes.add(propsBytes, innerTgtVertexId.bytes, opBytes)
    def propsKeyVal(labelId: Int, labelOrderSeq: Byte): Seq[(Int, DataVal)] = {
      val filtered = props.filter{ case (k, v) => k != HLabelMeta.emptyValue }
      if (filtered.isEmpty) {
        val opt = for {
          index <- HLabelIndex.findByLabelIdAndSeq(labelId, labelOrderSeq)
        } yield index.metaSeqs.map(x => x.toInt).zip(props.map(_._2))
        opt.getOrElse(List.empty[(Int, DataVal)])
      } else {
        filtered.toList
      }
    }
  }
}
