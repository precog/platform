package com.precog.yggdrasil
package jdbm3

import table._
import com.precog.common._ 
import com.precog.common.json._ 
import com.precog.util._ 
import com.precog.util.Bijection._

import org.apache.jdbm._

import org.joda.time.DateTime

import java.io._
import java.nio.ByteBuffer
import java.util.Map.Entry
import java.util.SortedMap
import Bijection._

import org.slf4j._

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

import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json.JsonParser
import blueeyes.json.Printer
import blueeyes.json.xschema._
import blueeyes.json.xschema.Extractor._
import blueeyes.json.xschema.DefaultSerialization._

object JDBMProjection {
  private[jdbm3] type IndexTree = SortedMap[Array[Byte],Array[Byte]]

  final val DEFAULT_SLICE_SIZE = 50000
  final val INDEX_SUBDIR = "jdbm"
    
  def isJDBMProjection(baseDir: File) = (new File(baseDir, INDEX_SUBDIR)).isDirectory
}

abstract class JDBMProjection (val baseDir: File, val descriptor: ProjectionDescriptor, sliceSize: Int = JDBMProjection.DEFAULT_SLICE_SIZE) extends BlockProjectionLike[Array[Byte], Slice] { projection =>
  import TableModule.paths._
  import JDBMProjection._

  val logger = LoggerFactory.getLogger("com.precog.yggdrasil.jdbm3.JDBMProjection")

  val keyColRefs = Array.tabulate(descriptor.identities) { i => ColumnRef(CPath(Key :: CPathIndex(i) :: Nil), CLong) }

  val keyFormat: IdentitiesRowFormat = RowFormat.IdentitiesRowFormatV1(keyColRefs)

  val rowFormat: RowFormat = RowFormat.ValueRowFormatV1(descriptor.columns map {
    case ColumnDescriptor(_, cPath, cType, _) => ColumnRef(cPath, cType)
  })

  override def toString = "JDBMProjection(" + descriptor.shows + ")"

  private[this] final val treeMapName = "byIdentityMap"

  private[this] final val indexDir = new File(baseDir, INDEX_SUBDIR)

  indexDir.mkdirs()

  implicit val keyOrder = IdentitiesOrder

  def setMDC() {
    MDC.put("projection", descriptor.shows)
  }

  protected lazy val idIndexFile: DB = try {
    logger.debug("Opening index file for " + toString + " from " + baseDir)
    DBMaker.openFile((new File(indexDir, "byIdentity")).getCanonicalPath).make()
  } catch {
    case t: Throwable => logger.error("Error on DB open", t); throw t
  }
  
  protected lazy val treeMap: IndexTree = {
    val treeMap: IndexTree = idIndexFile.getTreeMap(treeMapName)
    if (treeMap == null) {
      logger.debug("Creating new projection store")
      val ret: IndexTree = idIndexFile.createTreeMap(treeMapName, SortingKeyComparator(keyFormat, true), ByteArraySerializer, ByteArraySerializer)
      logger.debug("Created projection store")
      ret
    } else {
      logger.debug("Opening existing projection store")
      treeMap
    }
  }

  def close() = {
    setMDC()
    logger.debug("Closing column index files")
    idIndexFile.commit()
    idIndexFile.close()
    logger.debug("Closed column index files")
    MDC.clear()
  }

  def insert(ids : Identities, v : Seq[CValue], shouldSync: Boolean = false): IO[Unit] = IO {
    if (logger.isTraceEnabled) {
      logger.trace("Inserting %s => %s".format(ids.mkString("[", ", ", "]"), v))
    }

    treeMap.put(keyFormat.encodeIdentities(ids), rowFormat.encode(v.toList))

    if (shouldSync) {
      idIndexFile.commit()
    }
  }

  def getBlockAfter(id: Option[Array[Byte]], desiredColumns: Set[ColumnDescriptor] = Set()): Option[BlockProjectionData[Array[Byte],Slice]] = {
    setMDC()
    if (idIndexFile.isClosed()) {
      sys.error("Attempting to retrieve more data from a closed projection")
    }
    
    if (logger.isDebugEnabled) {
      logger.debug("Retrieving key after " + id.map(_.mkString("[", ", ", "]")))
    }

    val startTime = System.currentTimeMillis

    try {
      // tailMap semantics are >=, but we want > the IDs if provided
      val constrainedMap = id.map { idKey => treeMap.tailMap(idKey) } getOrElse treeMap
      val rawIterator = constrainedMap.entrySet.iterator.asScala
      if (id.isDefined && rawIterator.hasNext) rawIterator.next();
      
      if (rawIterator.isEmpty) {
        None 
      } else {
        val keyCols = Array.fill(descriptor.identities) { ArrayLongColumn.empty(sliceSize) }
        val valColumns = rowFormat.columnRefs.map(JDBMSlice.columnFor(CPath(Value), sliceSize))

        val keyColumnDecoder = keyFormat.ColumnDecoder(keyCols)
        val valColumnDecoder = rowFormat.ColumnDecoder(valColumns.map(_._2)(collection.breakOut))

        val (firstKey, lastKey, rows) = JDBMSlice.load(sliceSize, rawIterator, keyColumnDecoder, valColumnDecoder)

        val slice = new Slice {
          private val desiredRefs: Set[ColumnRef] = desiredColumns map { 
            case ColumnDescriptor(_, selector, tpe, _) => ColumnRef(CPath(Value) \ selector, tpe) 
          }
          
          val size = rows
          val columns = keyColRefs.iterator.zip(keyCols.iterator).toMap ++ valColumns.iterator.filter({
            case (ref @ ColumnRef(CPath(Value, _*), _), _) => desiredRefs.contains(ref)
            case _ => true
          })
        }

        Some(BlockProjectionData[Array[Byte],Slice](firstKey, lastKey, slice))
      }
    } catch {
      case e: java.util.NoSuchElementException => None
    } finally {
      if (logger.isDebugEnabled) {
        logger.debug("Block retrieved in %d ms".format(System.currentTimeMillis - startTime))
      }
      MDC.clear()
    }
  }
}

