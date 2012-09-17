package com.precog.yggdrasil
package jdbm3

import table._
import com.precog.common._ 
import com.precog.util._ 
import com.precog.util.Bijection._

import org.apache.jdbm._

import org.joda.time.DateTime

import java.io._
import java.nio.ByteBuffer
import java.util.Map.Entry
import java.util.SortedMap
import Bijection._

import com.weiglewilczek.slf4s.Logger
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import scalaz.{Ordering => _, _}
import scalaz.effect._
import scalaz.iteratee._
import scalaz.iteratee.Input._
import scalaz.syntax.plus._
import scalaz.syntax.monad._
import scalaz.syntax.applicativePlus._
import scalaz.syntax.bifunctor
import scalaz.syntax.show._
import scalaz.Scalaz._
import IterateeT._

import blueeyes.json.{ JPath, JPathIndex, JPathField }
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json.JsonParser
import blueeyes.json.Printer
import blueeyes.json.xschema._
import blueeyes.json.xschema.Extractor._
import blueeyes.json.xschema.DefaultSerialization._

object JDBMProjection {
  private[jdbm3] type IndexTree = SortedMap[Array[Long],Array[Byte]]

  final val DEFAULT_SLICE_SIZE = 10000
  final val INDEX_SUBDIR = "jdbm"
    
  def isJDBMProjection(baseDir: File) = (new File(baseDir, INDEX_SUBDIR)).isDirectory
}

abstract class JDBMProjection (val baseDir: File, val descriptor: ProjectionDescriptor, sliceSize: Int = JDBMProjection.DEFAULT_SLICE_SIZE) extends BlockProjectionLike[Identities, Slice] { projection =>
  import JDBMProjection._

  val logger = Logger("col:" + descriptor.shows)
  logger.debug("Opening column index files for projection " + descriptor.shows + " at " + baseDir)

  override def toString = "JDBMProjection(" + descriptor.columns + ")"

  private[this] final val treeMapName = "byIdentityMap"

  private[this] final val indexDir = new File(baseDir, INDEX_SUBDIR)

  indexDir.mkdirs()

  implicit val keyOrder = IdentitiesOrder

  protected lazy val idIndexFile: DB = try {
    logger.debug("Opening index file for " + toString)
    DBMaker.openFile((new File(indexDir, "byIdentity")).getCanonicalPath).make()
  } catch {
    case t: Throwable => logger.error("Error on DB open", t); throw t
  }
  
  protected lazy val treeMap: IndexTree = {
    val treeMap: IndexTree = idIndexFile.getTreeMap(treeMapName)
    if (treeMap == null) {
      logger.debug("Creating new projection store")
      val ret: IndexTree = idIndexFile.createTreeMap(treeMapName, AscendingIdentitiesComparator, null, null)
      logger.debug("Created projection store")
      ret
    } else {
      logger.debug("Opening existing projection store")
      treeMap
    }
  }

  def close() = {
    logger.info("Closing column index files")
    idIndexFile.commit()
    idIndexFile.close()
    logger.debug("Closed column index files")
  }

  val rowFormat: RowFormat = RowFormat.ValueRowFormatV1(descriptor.columns map {
    case ColumnDescriptor(_, cPath, cType, _) => ColumnRef(cPath, cType)
  })

  def insert(ids : Identities, v : Seq[CValue], shouldSync: Boolean = false): IO[Unit] = IO {
    logger.trace("Inserting %s => %s".format(ids, v))

    treeMap.put(ids.toArray, rowFormat.encode(v.toList))

    if (shouldSync) {
      idIndexFile.commit()
    }
  }

  def getBlockAfter(id: Option[Identities], desiredColumns: Set[ColumnDescriptor] = Set()): Option[BlockProjectionData[Identities,Slice]] = {
    import TableModule.paths._

    try {
      // tailMap semantics are >=, but we want > the IDs if provided
      val constrainedMap = id.map { idKey => treeMap.tailMap(idKey) } getOrElse treeMap
      val rawIterator = constrainedMap.entrySet.iterator.asScala
      if (id.isDefined && rawIterator.hasNext) rawIterator.next();
      
      if (rawIterator.isEmpty) {
        None 
      } else {
        val keyColRefs = Array.tabulate(descriptor.identities) { i => ColumnRef(JPath(Key :: JPathIndex(i) :: Nil), CLong) }
        val keyCols = Array.fill(descriptor.identities) { ArrayLongColumn.empty(sliceSize) }

        val valColumns = rowFormat.columnRefs.map(JDBMSlice.columnFor(JPath(Value), sliceSize))
        val keyColumnDecoder = (row: Int, key: Array[Long]) => {
          var i = 0
          while (i < key.length) { keyCols(i).update(row, key(i)); i += 1 }
        }

        val valColumnDecoder = rowFormat.ColumnDecoder(valColumns.map(_._2)(collection.breakOut))

        val (firstKey, lastKey, rows) = JDBMSlice.load(sliceSize, rawIterator, keyColumnDecoder, valColumnDecoder)

        val slice = new Slice {
          private val desiredRefs: Set[ColumnRef] = desiredColumns map { 
            case ColumnDescriptor(_, selector, tpe, _) => ColumnRef(JPath(Value) \ selector, tpe) 
          }
          
          val size = rows
          val columns = keyColRefs.iterator.zip(keyCols.iterator).toMap ++ valColumns.iterator.filter({
            case (ref @ ColumnRef(JPath(Value, _*), _), _) => desiredRefs.contains(ref)
            case _ => true
          })
        }

        Some(BlockProjectionData[Identities,Slice](firstKey, lastKey, slice))
      }
    } catch {
      case e: java.util.NoSuchElementException => None
    }
  }
}

