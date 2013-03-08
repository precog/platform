package com.precog.niflheim

import com.precog.common._
import com.precog.common.accounts.AccountId
import com.precog.common.ingest.EventId
import com.precog.common.json._
import com.precog.common.security.Authorities
import com.precog.util._

import com.weiglewilczek.slf4s.Logging

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

trait NIHDBSnapshot extends Logging {
  def blockIds: Array[Long]
  def readers: Array[StorageReader]

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
    logger.trace("findReaderAfter(%s) has i = %d, j = %d with blockIds.length = %d".format(id0, i, j, blockIds.length))
    if (j >= blockIds.length) None else Some(readers(j))
  }

  def getBlock(id0: Option[Long], cols: Option[Set[ColumnRef]]): Option[Block] =
    findReader(id0).map(_.snapshot(cols))

  def getBlockAfter(id0: Option[Long], cols: Option[Set[ColumnRef]]): Option[Block] =
    findReaderAfter(id0).map(_.snapshot(cols))

  def structure: Set[ColumnRef] =
    readers.map(_.structure.toSet).toSet.flatten

  def getConstraints(columns: Iterable[ColumnRef], cpaths: Set[ColumnRef]) = {
    columns.collect {
      case ColumnRef(cpath, ctype)
        if cpaths exists {
          case ColumnRef(p, t) => cpath.hasPrefix(p) && ctype == t
        } => ColumnRef(cpath, ctype)
    }
  }

  /**
   * Returns the total number of defined objects for a given `CPath` *mask*.
   * Since this punches holes in our rows, it is not simply the length of the
   * block. Instead we count the number of rows that have at least one defined
   * value at each path (and their children).
   */
  def count(id: Option[Long], refs0: Option[Set[ColumnRef]]): Option[Long] = {
    def countSegments(segs: Seq[Segment]): Long = segs.foldLeft(new BitSet) { (acc, seg) =>
      acc.or(seg.defined)
      acc
    }.cardinality

    findReader(id).map { reader =>
      refs0 map { refs =>
        val constraints = getConstraints(reader.structure, refs)
        val Block(_, cols, _) = reader.snapshot(Some(constraints.toSet))
        countSegments(cols)
      } getOrElse {
        reader.length
      }
    }
  }

  def count(refs0: Option[Set[ColumnRef]] = None): Long = {
    blockIds.foldLeft(0L) { (total, id) =>
      count(Some(id), refs0).getOrElse(0L)
    }
  }

  def reduce[A](reduction: Reduction[A], ref: ColumnRef): Map[CType, A] = {
    blockIds.foldLeft(Map.empty[CType, A]) { (acc, id) =>
      getBlock(Some(id), Some(Set(ref))) map { case Block(_, segments, _) =>
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
