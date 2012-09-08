package com.precog.yggdrasil
package jdbm3

import com.precog.common.Path
import com.precog.yggdrasil.table._

import blueeyes.json._

import org.apache.jdbm._
import org.joda.time.DateTime
import com.weiglewilczek.slf4s.Logging

import scalaz.effect.IO

import java.io.File
import java.util.SortedMap
import java.nio.ByteBuffer

import scala.collection.JavaConverters._

/**
 * A Projection wrapping a raw JDBM TreeMap index used for sorting. It's assumed that
 * the index has been created and filled prior to creating this wrapper.
 */
class JDBMRawSortProjection private[yggdrasil] (dbFile: File, indexName: String, sortKeyRefs: Seq[ColumnRef], valRefs: Seq[ColumnRef], sliceSize: Int = JDBMProjection.DEFAULT_SLICE_SIZE) extends BlockProjectionLike[Array[Byte],Slice] with Logging {

  // These should not actually be used in sorting
  def descriptor: ProjectionDescriptor = sys.error("Sort projections do not have full ProjectionDescriptors")
  def insert(id : Identities, v : Seq[CValue], shouldSync: Boolean = false): IO[Unit] = sys.error("Insertion on sort projections is unsupported")

  def foreach(f : java.util.Map.Entry[Array[Byte], Array[Byte]] => Unit) {
    val DB = DBMaker.openFile(dbFile.getCanonicalPath).make()
    val index: SortedMap[Array[Byte],Array[Byte]] = DB.getTreeMap(indexName)

    index.entrySet().iterator().asScala.foreach(f)

    DB.close()
  }

  private def keyAfter(k: Array[Byte]): Array[Byte] = {

    // TODO This won't be nearly as fast as Derek's, since JDBMSlice can no
    // longer get into the same level of detail about the encoded format. Is
    // this a problem? Should we allow writes w/ "holes?"

    val vals = keyFormat.decode(k)
    val last = vals.last match {
      case CLong(n) => CLong(n + 1)
      case v => sys.error("Expected a CLong (global ID) in the last position, but found " + v)
    }
    keyFormat.encode(vals.init :+ last)
  }

  val rowFormat = RowFormat.forValues(valRefs)
  val keyFormat = RowFormat.forValues(sortKeyRefs)

  def getBlockAfter(id: Option[Array[Byte]], columns: Set[ColumnDescriptor] = Set()): Option[BlockProjectionData[Array[Byte], Slice]] = {
    // TODO: Make this far, far less ugly
    if (columns.size > 0) {
      throw new IllegalArgumentException("JDBM Sort Projections may not be constrained by column descriptor")
    }

    val db = DBMaker.openFile(dbFile.getCanonicalPath).readonly().make()
    try {
      val index: SortedMap[Array[Byte],Array[Byte]] = db.getTreeMap(indexName)

      if (index == null) {
        throw new IllegalArgumentException("No such index in DB: %s:%s".format(dbFile, indexName))
      }

      val constrainedMap = id.map { idKey => index.tailMap(keyAfter(idKey)) }.getOrElse(index)
      if (constrainedMap.isEmpty) {
        None
      } else {
        var firstKey: Array[Byte] = null
        var lastKey: Array[Byte]  = null

        val slice = new JDBMSlice[Array[Byte]] {
          def requestedSize = sliceSize

          val keyColumns: Array[(ColumnRef, ArrayColumn[_])] = sortKeyRefs.map(JDBMSlice.columnFor(JPath("[0]"), sliceSize))(collection.breakOut)

          val valColumns: Array[(ColumnRef, ArrayColumn[_])] = valRefs.map(JDBMSlice.columnFor(JPath("[1]"), sliceSize))(collection.breakOut)

          val columnDecoder = rowFormat.ColumnDecoder(valColumns map (_._2))
          val keyColumnDecoder = keyFormat.ColumnDecoder(keyColumns map (_._2))

          def loadRowFromKey(row: Int, rowKey: Array[Byte]) {
            if (row == 0) { firstKey = rowKey }
            lastKey = rowKey

            keyColumnDecoder.decodeToRow(row, rowKey)
          }

          load(constrainedMap.entrySet.iterator.asScala)
        }

        if (firstKey == null) { // Just guard against an empty slice
          None
        } else {
          Some(BlockProjectionData[Array[Byte],Slice](firstKey, lastKey, slice))
        }
      }
    } finally {
      db.close() // creating the slice should have already read contents into memory
    }
  }
}
