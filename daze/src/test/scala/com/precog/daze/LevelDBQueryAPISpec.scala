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

class LevelDBQueryAPISpec extends Specification with LevelDBQueryComponent with StubYggShardComponent {
  implicit val actorSystem: ActorSystem = ActorSystem("leveldb_query_api_spec")
  implicit def asyncContext = ExecutionContext.defaultExecutionContext

  type YggConfig = LevelDBQueryConfig
  object yggConfig extends LevelDBQueryConfig {
    val projectionRetrievalTimeout = Timeout(intToDurationInt(10).seconds)
  }

  object storage extends Storage

  object query extends QueryAPI

  "combine" should {
    "restore objects from their component parts" in {
      val projectionData = storage.projections map { 
        case (pd, p) => ((pd.columns(0).selector, p.getAllPairs[Unit] map { _ map { case (ids, vs) =>  (ids, vs(0)) } })) 
      } toList

      import scalaz.{Order,Ordering}
      implicit def identityOrder[A, B]: (((Identities, A), (Identities, B)) => Ordering) = 
        (t1: (Identities, A), t2: (Identities, B)) => {
          (t1._1 zip t2._1).foldLeft[Ordering](Ordering.EQ) {
            case (Ordering.EQ, (i1, i2)) => Order[Long].order(i1, i2)
            case (ord, _) => ord
          }
        }

      implicit object SEventIdentityOrder extends Order[SEvent] {
        def order(s1: SEvent, s2: SEvent) = identityOrder(s1, s2)
      }
        
      val enum = query.combine(projectionData) map { _ map { case (ids, sv) => sv } }
      
      (consume[Unit, Vector[SValue], IO, List] &= enum[IO]).run(_ => sys.error("...")).unsafePerformIO.flatten must haveTheSameElementsAs(storage.sampleData.map(fromJValue))
    }
  }

  "fullProjection" should {
    "return all of the objects inserted into projections" in {
      val enum = Await.result(query.fullProjection[Unit](dataPath) map { case (ids, sv) => sv } fenum, intToDurationInt(30).seconds)
      
      (consume[Unit, Vector[SValue], IO, List] &= enum[IO]).run(_ => sys.error("...")).unsafePerformIO.flatten must haveTheSameElementsAs(storage.sampleData.map(fromJValue))
    }
  }

  "mask" should {
    "descend" in {
      val enum = Await.result(query.mask[Unit](dataPath).derefObject("gender").realize.fenum, intToDurationInt(30).seconds)
      val enumv = enum map { _ map { case (ids, sv) => sv } }
      (consume[Unit, Vector[SValue], IO, List] &= enumv[IO]).run(_ => sys.error("...")).unsafePerformIO.flatten must haveTheSameElementsAs(storage.sampleData.map(v => fromJValue(v \ "gender")))
    }
  }
}


// vim: set ts=4 sw=4 et:
