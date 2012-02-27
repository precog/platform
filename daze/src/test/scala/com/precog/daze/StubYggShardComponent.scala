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
package com.precog
package daze

import akka.actor._
import akka.dispatch._
import akka.util.Timeout
import akka.util.duration._

import blueeyes.json.JPath
import blueeyes.json.JsonAST._
import blueeyes.json.JsonParser

import com.precog.analytics._
import com.precog.common._
import com.precog.common.security._
import com.precog.common.util._
import com.precog.yggdrasil._
import com.precog.yggdrasil.shard._
import com.precog.yggdrasil.util._
import com.precog.util._
import SValue._

import scalaz.effect._
import scalaz.iteratee._
import scalaz.std.AllInstances._
import Iteratee._

import scala.collection.immutable.SortedMap
import scala.collection.immutable.TreeMap
import org.specs2.mutable._

trait StubYggShardComponent {
  def actorSystem: ActorSystem 
  implicit def asyncContext: ExecutionContext

  val dataPath = Path("/test")
  def sampleSize: Int

  trait Storage extends YggShard {

    def routingTable: RoutingTable = SingleColumnProjectionRoutingTable

    case class DummyProjection(descriptor: ProjectionDescriptor, data: SortedMap[Identities, Seq[CValue]]) extends Projection {
      val chunkSize = 2000

      def + (row: (Identities, Seq[CValue])) = copy(data = data + row)

      def getAllPairs[X] : EnumeratorP[X, Vector[(Identities, Seq[CValue])], IO] = {
        enumPStream[X, Vector[(Identities, Seq[CValue])], IO](data.grouped(chunkSize).map(c => Vector(c.toSeq: _*)).toStream)
      }

      def getAllValues[X] : EnumeratorP[X, Vector[Seq[CValue]], IO] = {
        enumPStream[X, Vector[Seq[CValue]], IO](data.values.grouped(chunkSize).map(c => Vector(c.toSeq: _*)).toStream)
      }

      def getAllIds[X] : EnumeratorP[X, Vector[Identities], IO] = {
        enumPStream[X, Vector[Identities], IO](data.keys.grouped(chunkSize).map(c => Vector(c.toSeq: _*)).toStream)
      }

      def getAllColumnPairs[X](columnIndex: Int) : EnumeratorP[X, Vector[(Identities, CValue)], IO] = {
        enumPStream[X, Vector[(Identities, CValue)], IO](data.map{case (i,v) => (i, v(columnIndex))}.grouped(chunkSize).map(c => Vector(c.toSeq: _*)).toStream)
      }

      def getPairsByIdRange[X](range: Interval[Identities]): EnumeratorP[X, Vector[(Identities, Seq[CValue])], IO] = sys.error("not needed")
    }

    val (sampleData, _) = DistributedSampleSet.sample(sampleSize, 0)

    val projections: Map[ProjectionDescriptor, Projection] = sampleData.zipWithIndex.foldLeft(Map.empty[ProjectionDescriptor, DummyProjection]) { 
      case (acc, (jobj, i)) => routingTable.route(EventMessage(EventId(0, i), Event(dataPath, "", jobj, Map()))).foldLeft(acc) {
        case (acc, ProjectionData(descriptor, identities, values, _)) =>
          acc + (descriptor -> (acc.getOrElse(descriptor, DummyProjection(descriptor, new TreeMap())) + ((identities, values))))
      }
    }

    def store(em: EventMessage) = sys.error("Feature not implemented in test stub.")

    def metadata = new StorageMetadata {
      implicit val dispatcher = actorSystem.dispatcher
      def findSelectors(path: Path): Future[Seq[JPath]] = 
        Future(projections.keys.flatMap(_.columns.filter(_.path == path).map(_.selector)).toSeq)

      def findProjections(path: Path, selector: JPath): Future[Map[ProjectionDescriptor, ColumnMetadata]] = 
        Future(projections.keys.flatMap(pd => pd.columns.collect { case cd @ ColumnDescriptor(`path`, `selector`, _, _) => (pd, ColumnMetadata.Empty) }).toMap)
    }

    def userMetadataView(uid: String) = new UserMetadataView(uid, UnlimitedAccessControl, metadata)(actorSystem.dispatcher)

    def projection(descriptor: ProjectionDescriptor)(implicit timeout: Timeout): Future[Projection] =
      Future(projections(descriptor))
  }
}



// vim: set ts=4 sw=4 et:
