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

import com.precog.common._
import com.precog.common.util._
import com.precog.yggdrasil._
import com.precog.yggdrasil.util._
import com.precog.util._
import SValue._

import scalaz.effect._
import scalaz.iteratee._
import scalaz.std.anyVal._
import scalaz.std.list._
import Iteratee._

import scala.collection.immutable.SortedMap
import scala.collection.immutable.TreeMap
import org.specs2.mutable._

class LevelDBQueryAPISpec extends Specification with LevelDBQueryComponent with StubYggShardComponent {

  implicit val actorSystem: ActorSystem = ActorSystem("leveldb_query_api_spec")
  implicit def asyncContext = ExecutionContext.defaultExecutionContext
  def sampleSize = 1 

  val testUID = "testUID"

  type YggConfig = LevelDBQueryConfig
  object yggConfig extends LevelDBQueryConfig {
    val projectionRetrievalTimeout = Timeout(intToDurationInt(10).seconds)
  }

  val storage = new Storage { }

  object query extends QueryAPI

  "fullProjection" should {
    "return all of the objects inserted into projections" in {
      val dataset = query.fullProjection(testUID, dataPath, System.currentTimeMillis + 10000)
      dataset.iterator.toSeq must haveTheSameElementsAs(storage.sampleData.map(fromJValue))
    }
  }

  "mask" should {
    "descend" in {
      val dataset = query.mask(testUID, dataPath).derefObject("gender").realize(System.currentTimeMillis + 10000)
      dataset.iterator.toSeq must haveTheSameElementsAs(storage.sampleData.map(v => fromJValue(v \ "gender")))
    }
  }
}


// vim: set ts=4 sw=4 et:
