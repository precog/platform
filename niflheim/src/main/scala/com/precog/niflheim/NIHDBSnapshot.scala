package com.precog.niflheim

import com.precog.common._
import com.precog.common.accounts.AccountId
import com.precog.common.ingest.EventId
import com.precog.common.security.Authorities
import com.precog.util._

import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.immutable.SortedMap

import java.util.Arrays

object NIHDBSnapshot {
  def apply(m: SortedMap[Long, StorageReader]): NIHDBSnapshot =
    new NIHDBSnapshot {
      val readers = m.values.filter(_.length > 0).toArray
      val blockIds = readers.map(_.id)
    }
}

trait NIHDBSnapshot {
  def blockIds: Array[Long]
  def readers: Array[StorageReader]

  val logger = LoggerFactory.getLogger("com.precog.niflheim.NIHDBSnapshot")

  protected[this] def findReader(id0: Option[Long]): Option[StorageReader] = {
    if (readers.isEmpty) {
      None
    } else {
      val i = id0.map(Arrays.binarySearch(blockIds, _)) getOrElse 0
      if (i >= 0) Some(readers(i)) else None
    }
  }

  protected[this] def findReaderAfter(id0: Option[Long]): Option[StorageReader] = {
    // be careful! the semantics of findReaderAfter are somewhat subtle
    val i = id0.map(Arrays.binarySearch(blockIds, _)) getOrElse -1
    val j = if (i < 0) -i - 1 else i + 1
    if (logger.isTraceEnabled) {
      logger.trace("findReaderAfter(%s) has i = %d, j = %d with blockIds.length = %d".format(id0, i, j, blockIds.length))
    }
    if (j >= blockIds.length) None else Some(readers(j))
  }

  def getBlock(id0: Option[Long], cols: Option[Set[CPath]]): Option[Block] =
    findReader(id0).map(_.snapshot(cols))

  def getBlockAfter(id0: Option[Long], cols: Option[Set[ColumnRef]]): Option[Block] =
    findReaderAfter(id0).map { reader =>
      val snapshot = reader.snapshotRef(cols)
      if (logger.isTraceEnabled) {
        logger.trace("Block after %s, %s (%s)\nSnapshot on %s:\n  %s".format(id0, reader, reader.hashCode, cols, snapshot.segments.map(_.toString).mkString("\n  ")))
      }
      snapshot
    }.orElse {
      if (logger.isTraceEnabled) {
        logger.trace("No block after " + id0)
      }
      None
    }

  def structure: Set[ColumnRef] = readers.flatMap(_.structure)(collection.breakOut)

  def getConstraints(columns: Iterable[ColumnRef], cpaths: Set[CPath]) = {
    columns.collect {
      case ColumnRef(cpath, _) if cpaths.exists(cpath.hasPrefix(_)) => cpath
    }
  }

  /**
   * Returns the total number of defined objects for a given `CPath` *mask*.
   * Since this punches holes in our rows, it is not simply the length of the
   * block. Instead we count the number of rows that have at least one defined
   * value at each path (and their children).
   */
  def count(id: Option[Long], paths0: Option[Set[CPath]]): Option[Long] = {
    def countSegments(segs: Seq[Segment]): Long = segs.foldLeft(new BitSet) { (acc, seg) =>
      acc.or(seg.defined)
      acc
    }.cardinality

    findReader(id).map { reader =>
      paths0 map { paths =>
        val constraints = getConstraints(reader.structure, paths)
        val Block(_, cols, _) = reader.snapshot(Some(constraints.toSet))
        countSegments(cols)
      } getOrElse {
        reader.length
      }
    }
  }

  def count(paths0: Option[Set[CPath]] = None): Long = {
    blockIds.foldLeft(0L) { (total, id) =>
      total + count(Some(id), paths0).getOrElse(0L)
    }
  }

  def reduce[A](reduction: Reduction[A], path: CPath): Map[CType, A] = {
    blockIds.foldLeft(Map.empty[CType, A]) { (acc, id) =>
      getBlock(Some(id), Some(Set(path))) map { case Block(_, segments, _) =>
        segments.foldLeft(acc) { (acc, segment) =>
          reduction.reduce(segment, None) map { a =>
            val key = segment.ctype
            val value = acc.get(key).map(reduction.semigroup.append(_, a)).getOrElse(a)
            acc + (key -> value)
          } getOrElse acc
        }
      } getOrElse acc
    }
  }
}
