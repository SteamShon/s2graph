package com.daumkakao.s2graph.core.types

import com.daumkakao.s2graph.core.{GraphUtil, DataVal}
import org.apache.hadoop.hbase.util.{PositionedByteRange, Bytes}

/**
 * Created by shon on 5/26/15.
 */
object VertexType {

  object VertexId {
    import DataVal._
    val defaultColId = 0
    val defaultInnerId = 0

    def apply(dataType: String)(pbr: PositionedByteRange, offset: Int, isEdge: Boolean, useHash: Boolean): VertexId = {
      var pos = offset
      if (useHash) pos += numOfBytesForHash
      val innerId = DataVal.valueOf(dataType)(pbr, pos)
      pos += innerId.bytes.length
      if (isEdge) VertexId(defaultColId, innerId, isEdge = isEdge, useHash = useHash)
      else {
        val columnId = Bytes.toInt(pbr.getBytes(), pos, 4)
        VertexId(columnId, innerId, isEdge = isEdge, useHash = useHash)
      }
    }
  }

  case class VertexId(columnId: Int, innerId: DataVal, isEdge: Boolean, useHash: Boolean) {
    lazy val hash = GraphUtil.murmur3(innerId.toString)
    lazy val hashBytes = Bytes.toBytes(hash)
    lazy val bytes = {
      val prefix = if (useHash) hashBytes else Array.empty[Byte]
      if (isEdge) {
        Bytes.add(prefix, innerId.bytes)
      } else {
        Bytes.add(prefix, innerId.bytes, Bytes.toBytes(columnId))
      }
    }
    lazy val bytesInUse = bytes.length

    def updateIsEdge(other: Boolean) = VertexId(columnId, innerId, other, useHash)

    def updateUseHash(other: Boolean) = VertexId(columnId, innerId, isEdge, other)

    override def equals(obj: Any) = {
      obj match {
        case other: VertexId => columnId == other.columnId && innerId == other.innerId
        case _ => false
      }
    }
  }

  object VertexRowKey {
    val isEdge = false

    def apply(dataType: String)(pbr: PositionedByteRange, offset: Int): VertexRowKey = {
      VertexRowKey(VertexId(dataType)(pbr, offset, isEdge = false, useHash = true))
    }
  }

  case class VertexRowKey(id: VertexId) {
    lazy val bytes = id.bytes
  }

  object VertexQualifier {
    def apply(bytes: Array[Byte], offset: Int): VertexQualifier = {
      VertexQualifier(bytes(offset))
    }
  }

  case class VertexQualifier(propKey: Byte) {
    lazy val bytes = Array.fill(1)(propKey)
  }

}

