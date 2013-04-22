/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
//package com.precog.yggdrasil
//package nihdb
//
//import com.precog.common._
//import com.precog.common.accounts._
//import com.precog.common.security.Authorities
//import com.precog.common.ingest._
//import com.precog.common.json._
//import com.precog.niflheim._
//import com.precog.util._
//import com.precog.yggdrasil.actor._
//import com.precog.yggdrasil.table._
//
//import akka.actor.{Actor, ActorRef, ActorSystem, Props}
//import akka.dispatch.{ExecutionContext, Future, Promise}
//import akka.pattern.{ask, GracefulStopSupport, pipe}
//import akka.util.{Duration, Timeout}
//
//import blueeyes.bkka.FutureMonad
//import blueeyes.json._
//
//import com.weiglewilczek.slf4s.Logging
//
//import scalaz._
//import scalaz.effect.IO
//import scalaz.std.list._
//import scalaz.syntax.traverse._
//
//import java.io.{File, FileNotFoundException, IOException}
//import java.util.concurrent.ScheduledExecutorService
//import java.util.concurrent.atomic.AtomicInteger
//
//import scala.collection.mutable
//import scala.collection.JavaConverters._
//
//object NIHDBProjection {
//  val projectionIdGen = new AtomicInteger()
//}
//
//trait NIHDBProjection {
//  def authorities: Future[Authorities]
//  def getSnapshot(): Future[NIHDBSnapshot]
//  def getBlockAfter(id0: Option[Long], columns: Option[Set[ColumnRef]])(implicit M: Monad[Future]): Future[Option[BlockProjectionData[Long, Slice]]]
//  def length: Future[Long]
//  def structure: Future[Set[ColumnRef]]
//  def status: Future[Status]
//  def commit: Future[PrecogUnit]
//  def close(implicit actorSystem: ActorSystem): Future[PrecogUnit]
//}
//
///**
//  *  Projection for NIH DB files
//  *
//  * @param cookThreshold The threshold, in rows, of raw data for cooking a raw store file
//  */
//class NIHDBActorProjection(val db: NIHDB)(implicit executor: ExecutionContext) extends NIHDBProjection with Logging { projection =>
//  // FIXME: projection IDs must be stable, globally unique, and assigned on creation!
//  private[this] val projectionId = NIHDBProjection.projectionIdGen.getAndIncrement
//
//  def authorities = db.authorities
//
//  // TODO: Rewrite NIHDBProjection to use a snapshot.
//  def getSnapshot(): Future[NIHDBSnapshot] = db.getSnapshot()
//
//  def getBlockAfter(id0: Option[Long], columns: Option[Set[ColumnRef]])(implicit M: Monad[Future]): Future[Option[BlockProjectionData[Long, Slice]]] = {
//    // FIXME: We probably want to change this semantic throughout Yggdrasil
//    db.getBlockAfter(id0, columns) map { block =>
//      block map { case Block(id, segs, stable) =>
//        BlockProjectionData[Long, Slice](id, id, SegmentsWrapper(segs, projectionId, id))
//      }
//    }
//  }
//
//  def insert(batches: Seq[(Long, Seq[IngestRecord])])(implicit M: Monad[Future]): Future[PrecogUnit] = {
//    db.insert(batches.map { case (offset, records) => (offset, records.map(_.value)) })
//  }
//
//  def length: Future[Long] = db.length
//
//  def structure: Future[Set[ColumnRef]] = {
//    db.structure map (_.map { case (cpath, ctype) =>
//      ColumnRef(cpath, ctype)
//    })
//  }
//
//  def status: Future[Status] = db.status
//
//  // NOOP. For now we sync *everything*
//  def commit: Future[PrecogUnit] = Promise.successful(PrecogUnit)
//
//  def close(implicit actorSystem: ActorSystem): Future[PrecogUnit] = db.close
//}
