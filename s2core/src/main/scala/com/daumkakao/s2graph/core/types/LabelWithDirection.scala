package com.daumkakao.s2graph.core.types

import com.daumkakao.s2graph.core.GraphUtil
import org.apache.hadoop.hbase.util.Bytes
import com.daumkakao.s2graph.core.DataVal.numOfBitsForDir

/**
 * Created by shon on 5/26/15.
 */
object LabelWithDirection {
  val maxBytes = Bytes.toBytes(Int.MaxValue)
  def apply(compositeInt: Int): LabelWithDirection = {

    val dir = compositeInt & ((1 << numOfBitsForDir) - 1)
    val labelId = compositeInt >> numOfBitsForDir
    LabelWithDirection(labelId, dir)
  }

}
case class LabelWithDirection(labelId: Int, dir: Int) {
  assert(dir < (1 << numOfBitsForDir))
  assert(labelId < (Int.MaxValue >> numOfBitsForDir))

  val labelBits = labelId << numOfBitsForDir

  lazy val compositeInt = labelBits | dir
  lazy val bytes = Bytes.toBytes(compositeInt)
  lazy val dirToggled = LabelWithDirection(labelId, GraphUtil.toggleDir(dir))
  def updateDir(newDir: Int) = LabelWithDirection(labelId, newDir)
}
