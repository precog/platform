package com.precog.yggdrasil
package nihdb

import akka.actor.ActorSystem
import akka.dispatch.Future

import blueeyes.json._

import com.precog.common._
import com.precog.common.security.Authorities
import com.precog.niflheim._
import com.precog.util.PrecogUnit
import com.precog.yggdrasil.table.{SegmentsWrapper, Slice}

import com.weiglewilczek.slf4s.Logging

import scalaz._

//class NIHDBAggregate(underlying: NonEmptyList[NIHDBSnapshot], authorities0: Authorities)(implicit M: Monad[Future]) extends NIHDB with Logging { parent =>
//  // FIXME: This is an awful way to use these. We should do offset computation to avoid duplicating these refs
//  private[this] val readers = underlying.list.map(_.readers).toArray.flatten
//
//  // Precompute some static info
//  private[this] val snapshotStructure: Set[(CPath, CType)] = readers.map(_.structure).toSet.flatten
//  private[this] val snapshotLength = readers.map(_.length.toLong).sum
//
//  val projection = new NIHDBProjection {
//    private[this] val projectionId = NIHDB.projectionIdGen.getAndIncrement
//
//    val structure = M.point(parent.snapshotStructure.map { case (s, t) => ColumnRef(s, t) })
//    val length = parent.snapshotLength
//
//    def getBlockAfter(id0: Option[Long], columns: Option[Set[ColumnRef]])(implicit MP: Monad[Future]): Future[Option[BlockProjectionData[Long, Slice]]] = MP.point {
//      val id = id0.map(_ + 1)
//      val index = id getOrElse 0L
//      parent.getSnapshotBlock(id, columns.map(_.map(_.selector))) map {
//        case Block(_, segments, _) =>
//          val slice = SegmentsWrapper(segments, projectionId, index)
//          BlockProjectionData(index, index, slice)
//      }
//    }
//
//    def reduce[A](reduction: Reduction[A], path: CPath): Map[CType, A] = {
//      parent.readers.foldLeft(Map.empty[CType, A]) { (acc, reader) =>
//        reader.snapshot(Some(Set(path))).segments.foldLeft(acc) { (acc, segment) =>
//          reduction.reduce(segment, None) map { a =>
//            val key = segment.ctype
//            val value = acc.get(key).map(reduction.semigroup.append(_, a)).getOrElse(a)
//            acc + (key -> value)
//          } getOrElse acc
//        }
//      }
//    }
//  }
//
//  val authorities: Future[Authorities] = M.point(authorities0)
//
//  def insert(batch: Seq[(Long, Seq[JValue])]): Future[PrecogUnit] = sys.error("Insert unsupported in aggregate")
//
//  def getSnapshot(): Future[NIHDBSnapshot] = sys.error("Snapshot unsupported in aggregate")
//
//  def getBlockAfter(id: Option[Long], columns: Option[Set[ColumnRef]]): Future[Option[Block]] = {
//    getBlock(id.map(_ + 1), columns.map(_.map(_.selector)))
//  }
//
//  def getBlock(id: Option[Long], columns: Option[Set[CPath]]): Future[Option[Block]] =
//    M.point(getSnapshotBlock(id, columns))
//
//  private def getSnapshotBlock(id: Option[Long], columns: Option[Set[CPath]]): Option[Block] = {
//    try {
//      // We're limiting ourselves to 2 billion blocks total here
//      val index = id.map(_.toInt).getOrElse(0)
//      if (index >= readers.length) {
//        None
//      } else {
//        Some(Block(index, readers(index).snapshot(columns).segments, true))
//      }
//    } catch {
//      case e =>
//        // Difficult to do anything else here other than bail
//        logger.warn("Error during block read", e)
//        None
//    }
//  }
//
//  def length: Future[Long] = M.point(snapshotLength)
//
//  def status: Future[Status] = sys.error("Status unsupported in aggregate")
//
//  def structure: Future[Set[(CPath, CType)]] = M.point(snapshotStructure)
//
//  def count(paths0: Option[Set[CPath]]): Future[Long] = M.point(underlying.map(_.count(paths0)).list.sum)
//
//  def close(implicit actorSystem: ActorSystem): Future[PrecogUnit] = M.point(PrecogUnit)
//}
//
