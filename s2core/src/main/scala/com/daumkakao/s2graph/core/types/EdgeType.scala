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
  def bytesToProps(dataTypes: Map[Int, String])(pbr: PositionedByteRange, offset: Int) = {
    val bytes = pbr.getBytes()
    var pos = offset
    val len = bytes(pos)
    assert(dataTypes.size == len)
    pos += 1
    val kvs = for (i <- (0 until len)) yield {
      val k = bytes(pos)
      val dataType = dataTypes(k)
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
    def apply(propsDataTypes: Map[Int, String], dataType: String)(pbr: PositionedByteRange, offset: Int): EdgeQualifier = {
      val bytes = pbr.getBytes()
      var pos = offset

      val (props, tgtVertexId) = {
        val (props, endAt) = bytesToProps(propsDataTypes)(pbr, pos)
        val propsMap = props.toMap
        val idVal =
          if (propsMap.contains(HLabelMeta.toSeq)) propsMap(HLabelMeta.toSeq)
          else DataVal(dataType)(pbr, endAt)

        val tgtVertexId = VertexId(VertexId.defaultColId, idVal, isEdge = true, useHash = false)
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
    lazy val bytes = {
      /** check if props already have targetVertexId information.
        * when _to is used in indexProps, props can hold tgtVertexId
        * */
      if (props.contains((HLabelMeta.toSeq -> tgtVertexId.innerId))) {
        Bytes.add(propsBytes, opBytes)
      } else {
        Bytes.add(propsBytes, innerTgtVertexId.bytes, opBytes)
      }
    }
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

  object EdgeValue {
    def apply(dataTypes: Map[Int, String])(pbr: PositionedByteRange, offset: Int): EdgeValue = {
      val (props, endAt) = bytesToProps(dataTypes)(pbr, offset)
      EdgeValue(props)
    }
  }
  case class EdgeValue(props: Seq[(Int, DataVal)]) {
    lazy val bytes = propsToBytes(props)
  }

  /** snapshot edge */
  object EdgeQualifierInverted {
    def apply(dataType: String)(pbr: PositionedByteRange, offset: Int, len: Int): EdgeQualifierInverted = {
      val bytes = pbr.getBytes()
      val tgtVertexId = VertexId(dataType)(pbr, offset, isEdge = true, useHash = false)
      val propKey = bytes(offset + len - 1)
      EdgeQualifierInverted(tgtVertexId, propKey)
    }
  }
  case class EdgeQualifierInverted(tgtVertexId: VertexId, propKey: Byte) {
    assert(Byte.MinValue <= propKey && propKey <= Byte.MaxValue)
    val innerTgtVertexId = tgtVertexId.updateUseHash(false)
    lazy val bytes = Bytes.add(innerTgtVertexId.bytes, Array.fill(1)(propKey))
  }
  object EdgeValueInverted {
    def apply(dataType: String)(pbr: PositionedByteRange, offset: Int): EdgeValueInverted = {
      val bytes = pbr.getBytes()
      val op = bytes(offset)
      val propVal = DataVal(dataType)(pbr, offset + 1)
      EdgeValueInverted(op, propVal)
    }
  }
  case class EdgeValueInverted(op: Byte, propVal: DataVal) {
    lazy val bytes = Bytes.add(Array.fill(1)(op), propVal.bytes)
  }
}
