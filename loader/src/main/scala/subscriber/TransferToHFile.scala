package subscriber


import com.kakao.s2graph.core._
import com.kakao.s2graph.core.mysqls.{LabelMeta, Label}
import com.kakao.s2graph.core.types.{InnerValLikeWithTs, LabelWithDirection, SourceVertexId}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
import org.apache.hadoop.hbase.mapreduce.{TableOutputFormat}
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkContext}
import org.apache.spark.rdd.RDD
import org.hbase.async.{AtomicIncrementRequest, DeleteRequest, HBaseRpc, PutRequest}
import play.api.libs.json.Json
import s2.spark.{HashMapParam, SparkApp}
import spark.{FamilyHFileWriteOptions, KeyFamilyQualifier, HBaseContext}
import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * transform tsv format edges into key value.
 */
class TsvToKeyValue extends JSONParser {

  private def rpcToKeyValues(rpc: HBaseRpc): Seq[KeyValue] = {
    rpc match {
      case p: PutRequest => Seq(new KeyValue(p.key(), p.family(), p.qualifier, p.timestamp, p.value))
      case d: DeleteRequest => d.qualifiers().toSeq.map { qualifier => new KeyValue(d.key(), d.family(), qualifier, d.timestamp(), Array.empty[Byte]) }
      case _ => throw new RuntimeException(s"not supported rpc type. rpc should be in (PutRequest, DeleteRequest). $rpc")
    }
  }

  def toKeyValue(s: String, labelMapping: Map[String, String], autoEdgeCreate: Boolean): Seq[KeyValue] = {
    Graph.toGraphElement(s, labelMapping).toSeq.flatMap { element =>
      element match {
        case e: Edge =>
          val relEdges = if (autoEdgeCreate) e.relatedEdges else Seq(e)
          val indexEdgeRpcs = relEdges.flatMap { relEdge =>
            relEdge.edgesWithIndex.flatMap { indexEdge =>
              val rpcs = indexEdge.op match {
                case i if i == GraphUtil.operations("insertBulk") | i == GraphUtil.operations("insert") =>
                  GraphSubscriberHelper.builder.buildPutsAsync(indexEdge)
                //                  ++ GraphSubscriberHelper.builder.buildIncrementsAsync(indexEdge)
                case d if d == GraphUtil.operations("delete") =>
                  GraphSubscriberHelper.builder.buildDeletesAsync(indexEdge)
                //                  ++ GraphSubscriberHelper.builder.buildIncrementsAsync(indexEdge, -1L)
              }

              rpcs.flatMap { rpc => rpcToKeyValues(rpc) }
            }
          }
          val snapshotEdgeRpcs = GraphSubscriberHelper.builder.buildPutAsync(e.toSnapshotEdge).flatMap { rpc => rpcToKeyValues(rpc) }
          indexEdgeRpcs ++ snapshotEdgeRpcs
        case v: Vertex =>
          // not support vertex yet. use self edge for now.
          Seq.empty
        case _ => throw new RuntimeException(s"not supported graph element type. $s")
      }
    }
  }
}


case class DegreeKey(vertexIdStr: String, labelName: String, direction: String) extends JSONParser {
  def label = Label.findByName(labelName).getOrElse(throw new RuntimeException(s"$labelName is not found in DB."))
  def dir = GraphUtil.directions.get(direction).getOrElse(throw new RuntimeException(s"$direction is not supported."))
  def labelWithDir = LabelWithDirection(label.id.get, dir)
  def vertexIdVal = jsValueToInnerVal(Json.toJson(vertexIdStr), label.srcColumnWithDir(dir).columnType, label.schemaVersion)
    .getOrElse(throw new RuntimeException(s"$vertexIdStr can not be converted into innerVal."))
  def vertex = Vertex(SourceVertexId(label.srcColumn.id.get, vertexIdVal))
  def ts = System.currentTimeMillis()
  def propsWithTs = Map(LabelMeta.timeStampSeq -> InnerValLikeWithTs.withLong(ts, ts, label.schemaVersion))

  def toEdge = Edge(vertex, vertex, labelWithDir, propsWithTs = propsWithTs)
}

object TransferToHFile extends SparkApp {

  val usages =
    s"""
       |create HFiles for hbase table on zkQuorum specified.
       |note that hbase table is created already and pre-splitted properly.
       |
       |params:
       |0. input: hdfs path for tsv file(bulk format).
       |1. output: hdfs path for storing HFiles.
       |2. zkQuorum: running hbase cluster zkQuorum.
       |3. tableName: table name for this bulk upload.
       |4. dbUrl: db url for parsing to graph element.
     """.stripMargin

  //TODO: Process AtomicIncrementRequest too.


  val parser = new TsvToKeyValue()


  def buildDegrees(msgs: RDD[String], labelMapping: Map[String, String], edgeAutoCreate: Boolean): RDD[(DegreeKey, Long)] = {
    for {
      msg <- msgs
      tokens = GraphUtil.split(msg)
      if tokens(2) == "e" || tokens(2) == "edge"
      tempDirection = if (tokens.length == 7) "out" else tokens(7)
      direction = if (tempDirection != "out" && tempDirection != "in") "out" else tempDirection
      reverseDirection = if (direction == "out") "in" else "out"
      convertedLabelName = labelMapping.get(tokens(5)).getOrElse(tokens(5))
      (vertexIdStr, vertexIdStrReversed) = (tokens(3), tokens(4))
      degreeKey = DegreeKey(vertexIdStr, convertedLabelName, direction)
      degreeKeyReversed = DegreeKey(vertexIdStrReversed, convertedLabelName, reverseDirection)
      op = tokens(1)
      degreeVal = if (op == "insert" || op == "insertBulk") 1L else if (op == "delete") -1L else 0
      extra = if (edgeAutoCreate) List(degreeKeyReversed -> degreeVal) else Nil
      output <- List(degreeKey -> degreeVal) ++ extra
    } yield output
  }

  def toKeyValues(strs: Seq[String], labelMapping: Map[String, String], autoEdgeCreate: Boolean): Iterator[KeyValue] = {
    val kvs = for {
      s <- strs
      kv <- parser.toKeyValue(s, labelMapping, autoEdgeCreate)
    } yield kv

    kvs.toIterator
  }

  def mutateOnHTable(hTable: HTableInterface, rpcs: Seq[HBaseRpc]): Unit = {
    rpcs.foreach { rpc =>
      rpc match {
        case inc: AtomicIncrementRequest =>
          val newInc = new Increment(inc.key())
          newInc.addColumn(inc.family(), inc.qualifier(), inc.getAmount)
          hTable.increment(newInc)
        case put: PutRequest =>
          val newPut = new Put(put.key())
          newPut.addColumn(put.family(), put.qualifier(), put.timestamp(), put.value())
          hTable.put(newPut)
        case del: DeleteRequest =>
          val newDelete = new Delete(del.key())
          del.qualifiers().foreach { qualifier =>
            newDelete.addColumn(del.family(), qualifier, del.timestamp())
          }
          hTable.delete(newDelete)
        case _ => throw new RuntimeException(s"not supported rpc type: $rpc")
      }
    }
  }

  def incrementToPut(incr: AtomicIncrementRequest): PutRequest =
    new PutRequest(incr.table(), incr.key(), incr.family(), incr.qualifier(), Bytes.toBytes(incr.getAmount))

  def hbaseConf(zkQuorum: String, tableName: String): Configuration = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", zkQuorum)
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    conf.set("hadoop.tmp.dir", s"/tmp/$tableName")
    conf
  }

  override def run() = {
    val input = args(0)
    val tmpPath = args(1)
    val zkQuorum = args(2)
    val tableName = args(3)
    val dbUrl = args(4)
    val maxHFilePerResionServer = args(5).toInt
    val labelMapping = if (args.length >= 7) GraphSubscriberHelper.toLabelMapping(args(6)) else Map.empty[String, String]
    val autoEdgeCreate = if (args.length >= 8) args(7).toBoolean else false
    val buildDegree = if (args.length >= 9) args(8).toBoolean else true
    val compressionAlgorithm = if (args.length >= 10) args(9) else "lz4"
    val incrementalLoad = if (args.length >= 11) args(10).toBoolean else false

    val conf = sparkConf(s"$input: TransferToHFile")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryoserializer.buffer.mb", "24")

    val sc = new SparkContext(conf)
    val mapAcc = sc.accumulable(mutable.HashMap.empty[String, Long], "counter")(HashMapParam[String, Long](_ + _))

    Management.createTable(zkQuorum, tableName, List("e", "v"), maxHFilePerResionServer, None, compressionAlgorithm)

    /** set up hbase configuration */
    val hConf = hbaseConf(zkQuorum, tableName)

    val rdd = sc.textFile(input)

    /** build HFile from edges in TSV */
    val kvs = rdd.mapPartitions { iter =>
      val lines = iter.toSeq
      mapAcc += ("numberOfEdges" -> lines.size)
      val phase = System.getProperty("phase")
      GraphSubscriberHelper.apply(phase, dbUrl, "none", "none")
      toKeyValues(lines, labelMapping, autoEdgeCreate)
    }

    def flatMap(kv: KeyValue): Iterator[(KeyFamilyQualifier, Array[Byte])] = {
      val k = new KeyFamilyQualifier(kv.getRow(), kv.getFamily(), kv.getQualifier())
      val v = kv.getValue()
      Seq((k -> v)).toIterator
    }
    val familyOptions = new FamilyHFileWriteOptions(Algorithm.LZ4.getName.toUpperCase,
      BloomType.ROW.name().toUpperCase, 32768, DataBlockEncoding.FAST_DIFF.name().toUpperCase)
    val familyOptionsMap = Map("e".getBytes() -> familyOptions, "v".getBytes() -> familyOptions)

    val hbaseSc = new HBaseContext(sc, hConf)
    hbaseSc.bulkLoad(kvs, TableName.valueOf(tableName), flatMap, tmpPath, familyOptionsMap)

    /** more work for degree vals */
    if (buildDegree) {
      val degreeVals = buildDegrees(rdd, labelMapping, autoEdgeCreate).reduceByKey { (agg, current) => agg + current }
      degreeVals.foreachPartition { iter =>
        /**
         * setup hbase connection.
         * use native hbase client since some hadoop cluster have conflict on guava version.
         */
        val hConf = hbaseConf(zkQuorum, tableName)
        val conn = HConnectionManager.createConnection(hConf)
        val hTable = conn.getTable(TableName.valueOf(tableName))
        try {
          iter.foreach { case (degreeKey, degreeVal) =>
            mapAcc += ("numberOfDegrees" -> 1L)
            val edge = degreeKey.toEdge
            val relEdges = if (autoEdgeCreate) edge.relatedEdges else List(edge)
            relEdges.foreach { relEdge =>
              relEdge.edgesWithIndex.foreach { indexEdge =>
                val incrs = GraphSubscriberHelper.builder.buildIncrementsAsync(indexEdge, degreeVal)
                val rpcs =
                  if (incrementalLoad) incrs
                  else {
                    for {
                      rpc <- incrs
                      incr = rpc.asInstanceOf[AtomicIncrementRequest]
                    } yield incrementToPut(incr)
                  }
                mutateOnHTable(hTable, rpcs)
              }
            }
          }
        } finally {
          hTable.close()
          conn.close()
        }
      }
    }
  }
}




