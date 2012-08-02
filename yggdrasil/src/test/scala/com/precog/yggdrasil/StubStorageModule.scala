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
package com.precog.yggdrasil

import actor._
import metadata._
import util._
import com.precog.util._
import SValue._
import com.precog.common._
import com.precog.common.security._
import com.precog.common.util._

import akka.actor.ActorSystem
import akka.dispatch._
import akka.testkit.TestActorRef
import akka.util.Timeout
import akka.util.duration._

import blueeyes.json.JPath
import blueeyes.json.JsonAST._
import blueeyes.json.JsonParser

import scalaz._
import scalaz.effect._

import scala.collection.immutable.SortedMap
import scala.collection.immutable.TreeMap


trait StubStorageModule[M[+_]] extends StorageModule[M] { self =>
  type TestDataset

  implicit def M: Monad[M]

  def projections: Map[ProjectionDescriptor, Projection]

  class Storage extends StorageLike {
    def storeBatch(ems: Seq[EventMessage]) = sys.error("Feature not implemented in test stub.")

    def projectionMetadata: Map[ProjectionDescriptor, ColumnMetadata] = 
      projections.keys.map(pd => (pd, ColumnMetadata.Empty)).toMap

    def metadata = new StorageMetadata[M] {
      val M = self.M
      val source = new TestMetadataStorage(projectionMetadata)
      def findChildren(path: Path) = M.point(source.findChildren(path))
      def findSelectors(path: Path) = M.point(source.findSelectors(path))
      def findProjections(path: Path, selector: JPath) = M.point {
        projections.collect {
          case (descriptor, _) if descriptor.columns.exists { case ColumnDescriptor(p, s, _, _) => p == path && s == selector } => 
            (descriptor, ColumnMetadata.Empty)
        }
      }

      def findPathMetadata(path: Path, selector: JPath) = M.point(source.findPathMetadata(path, selector).unsafePerformIO)
    }

    def userMetadataView(uid: String) = new UserMetadataView[M](uid, new UnlimitedAccessControl(), metadata)

    def projection(descriptor: ProjectionDescriptor) = M.point(projections(descriptor) -> new Release(IO(())))
  }
}


trait DistributedSampleStubStorageModule[M[+_]] extends StubStorageModule[M] {
  val dataPath = Path("/test")
  def sampleSize: Int
  def dataset(idCount: Int, data: Iterable[(Identities, Seq[CValue])]): TestDataset

  // TODO: This duplicates the same class in com.precog.muspelheim.RawJsonShardComponent
  case class Projection(descriptor: ProjectionDescriptor, data: SortedMap[Identities, Seq[CValue]]) extends FullProjectionLike[TestDataset] {
    val chunkSize = 2000

    def insert(id : Identities, v : Seq[CValue], shouldSync: Boolean = false): IO[Unit] = sys.error("Dummy ProjectionLike doesn't support insert")      

    def allRecords(expiresAt: Long): TestDataset = dataset(1, data)
  }

  implicit lazy val ordering = IdentitiesOrder.toScalaOrdering

  lazy val routingTable: RoutingTable = new SingleColumnProjectionRoutingTable

  lazy val sampleData: Vector[JValue] = DistributedSampleSet.sample(sampleSize, 0)._1

  lazy val projections: Map[ProjectionDescriptor, Projection] = sampleData.zipWithIndex.foldLeft(Map.empty[ProjectionDescriptor, Projection]) { 
    case (acc, (jobj, i)) => routingTable.route(EventMessage(EventId(0, i), Event(dataPath, "", jobj, Map()))).foldLeft(acc) {
      case (acc, ProjectionData(descriptor, values, _)) =>
        acc + (descriptor -> (Projection(descriptor, acc.get(descriptor).map(_.data).getOrElse(TreeMap.empty(ordering)) + (VectorCase(EventId(0,i).uid) -> values))))
    }
  }
}


// vim: set ts=4 sw=4 et:
