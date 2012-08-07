package com.precog.yggdrasil
package jdbm3

import com.precog.common.Path
import com.precog.yggdrasil.table._

import blueeyes.json.{JPath,JPathIndex}
import org.apache.jdbm._
import com.weiglewilczek.slf4s.Logging

import scalaz.effect.IO

import java.io.File
import java.util.SortedMap

import scala.collection.JavaConverters._

/**
 * A Projection wrapping a raw JDBM TreeMap index used for sorting. It's assumed that
 * the index has been created and filled prior to creating this wrapper.
 */
abstract class JDBMRawSortProjection private[yggdrasil] (dbFile: File, indexName: String, idCount: Int, keyRefs: Seq[ColumnRef], valRefs: Seq[ColumnRef], sliceSize: Int = JDBMProjection.DEFAULT_SLICE_SIZE) extends BlockProjectionLike[SortingKey,Slice] with Logging {
  // These should not actually be used in sorting
  def descriptor: ProjectionDescriptor = sys.error("Sort projections do not have full ProjectionDescriptors")
  def insert(id : Identities, v : Seq[CValue], shouldSync: Boolean = false): IO[Unit] = sys.error("Insertion on sort projections is unsupported")

  def getBlockAfter(id: Option[SortingKey], columns: Set[ColumnDescriptor] = Set()): Option[BlockProjectionData[SortingKey,Slice]] = try {
    import TableModule.paths._

    // TODO: Make this far, far less ugly
    if (columns.size > 0) {
      throw new IllegalArgumentException("JDBM Sort Projections may not be constrained by column descriptor")
    }

    val DB = DBMaker.openFile(dbFile.getCanonicalPath).make()
    val index: SortedMap[SortingKey,Array[CValue]] = DB.getTreeMap(indexName)

    if (index == null) {
      throw new IllegalArgumentException("No such index in DB: %s:%s".format(dbFile, indexName))
    }

    val constrainedMap = id.map { idKey => index.tailMap(idKey.copy(index = idKey.index + 1)) }.getOrElse(index)
    
    constrainedMap.lastKey() // should throw an exception if the map is empty, but...

    val slice = new ArrayRowJDBMSlice[SortingKey] {
      def source = constrainedMap.entrySet.iterator.asScala
      def requestedSize = sliceSize

      import blueeyes.json.{JPathField,JPathIndex}

      case class IdentColumn(index: Int) extends LongColumn {
        def isDefinedAt(row: Int) = row >= 0 && row < backing.length
        def apply(row: Int): Long = backing(row).getKey.ids.apply(index)
      }
      
      def keyColumns: Map[ColumnRef,Column] = (0 until idCount).map {
        idx: Int => ColumnRef(JPath(Key :: JPathIndex(idx) :: Nil), CLong) -> IdentColumn(idx)
      }.toMap

      def sortKeyColumns: Map[ColumnRef,Column] = keyRefs.zipWithIndex.map {
        case (ColumnRef(selector, tpe),index) => 
          columnFor(row => backing(row).getKey.columns, ColumnRef(JPath(SortKey) \ selector, tpe), index)
      }.toMap
      
      def valColumns: Map[ColumnRef,Column] = valRefs.zipWithIndex.map {
        case (ColumnRef(selector, tpe), index) =>
          columnFor(row => backing(row).getValue, ColumnRef(JPath(Value) \ selector, tpe), index)
      }.toMap

      lazy val columns = keyColumns ++ sortKeyColumns ++ valColumns
    }

    logger.debug("Backing slice created of size " + slice.size)

    DB.close() // creating the slice should have already read contents into memory

    Some(BlockProjectionData[SortingKey,Slice](slice.firstKey, slice.lastKey, slice))
  } catch {
    case e: java.util.NoSuchElementException => None
  }
}
