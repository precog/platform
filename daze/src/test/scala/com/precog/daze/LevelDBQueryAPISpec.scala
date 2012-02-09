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
import com.precog.common.util._
import com.precog.yggdrasil._
import com.precog.yggdrasil.shard._
import com.precog.yggdrasil.util._
import com.precog.util._
import StorageMetadata._
import SValue._

import scalaz.effect._
import scalaz.iteratee._
import scalaz.std.AllInstances._
import Iteratee._

import scala.collection.immutable.SortedMap
import scala.collection.immutable.TreeMap
import org.specs2.mutable._

class LevelDBQueryAPISpec extends Specification with LevelDBQueryComponent {
  val dataPath = Path("/test")
  implicit val actorSystem: ActorSystem = ActorSystem("leveldb_query_api_spec")
  implicit def asyncContext = ExecutionContext.defaultExecutionContext

  type YggConfig = LevelDBQueryConfig
  object yggConfig extends LevelDBQueryConfig {
    val projectionRetrievalTimeout = Timeout(intToDurationInt(10).seconds)
  }

  object storage extends YggShard {
    def routingTable: RoutingTable = SingleColumnProjectionRoutingTable

    object IdentitiesOrdering extends scala.math.Ordering[Identities] {
      override def compare(id1: Identities, id2: Identities) = {
        (id1 zip id2).foldLeft(0) {
          case (0, (id1, id2)) => id1 compare id2
          case (other, _) => other
        }
      }
    }

    case class DummyProjection(descriptor: ProjectionDescriptor, data: SortedMap[Identities, Seq[CValue]]) extends Projection {
      def + (row: (Identities, Seq[CValue])) = copy(data = data + row)

      def getAllPairs[X] : EnumeratorP[X, (Identities, Seq[CValue]), IO] = {
        enumPStream[X, (Identities, Seq[CValue]), IO](data.toStream)
      }

      def getPairsByIdRange[X](range: Interval[Identities]): EnumeratorP[X, (Identities, Seq[CValue]), IO] = sys.error("not needed")
    }

    val (sampleData, _) = DistributedSampleSet.sample(5, 0)

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

    def projection(descriptor: ProjectionDescriptor)(implicit timeout: Timeout): Future[Projection] =
      Future(projections(descriptor))
  }

  object query extends QueryAPI

  "combine" should {
    "restore objects from their component parts" in {
      val projectionData = storage.projections map { 
        case (pd, p) => ((pd.columns(0).selector, p.getAllPairs[Unit] map { case (ids, vs) =>  (ids, vs(0)) })) 
      } toList

      val enum = query.combine(projectionData) map { case (ids, sv) => sv }
      
      (consume[Unit, SValue, IO, List] &= enum[IO]).run(_ => sys.error("...")).unsafePerformIO must haveTheSameElementsAs(storage.sampleData.map(fromJValue))
    }
  }

  "fullProjection" should {
    "return all of the objects inserted into projections" in {
      val enum = Await.result(query.fullProjection[Unit](dataPath) map { case (ids, sv) => sv } fenum, intToDurationInt(30).seconds)
      
      (consume[Unit, SValue, IO, List] &= enum[IO]).run(_ => sys.error("...")).unsafePerformIO must haveTheSameElementsAs(storage.sampleData.map(fromJValue))
    }
  }
}


// vim: set ts=4 sw=4 et:
