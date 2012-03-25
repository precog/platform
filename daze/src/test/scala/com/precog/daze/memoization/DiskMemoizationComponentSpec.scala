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
package com.precog.daze
package memoization

import akka.actor.ActorSystem
import akka.dispatch.ExecutionContext
import akka.dispatch.Await
import akka.util.Timeout
import akka.util.duration._

import blueeyes.json.JsonAST._

import com.precog.common._
import com.precog.common.util._
import com.precog.yggdrasil._
import com.precog.yggdrasil.serialization._
import com.precog.yggdrasil.util._
import com.precog.util._
import SValue._

import java.io.File
import scalaz.effect._
import scalaz.iteratee._
import scalaz.std.list._
import Iteratee._

import org.specs2.mutable._
import org.specs2.ScalaCheck

class DiskMemoizationComponentSpec extends Specification with DiskMemoizationComponent with StubYggShardComponent with ScalaCheck with ArbitrarySValue {
  type X = Throwable
  implicit val actorSystem: ActorSystem = ActorSystem("leveldb_memoization_spec")
  implicit def asyncContext = ExecutionContext.defaultExecutionContext
  implicit val timeout = Timeout(intToDurationInt(30).seconds)
  implicit val chunkSerialization = new BinaryProjectionSerialization with IterateeFileSerialization[Vector[SEvent]]
  def sampleSize = 50

  type YggConfig = DiskMemoizationConfig 
  object yggConfig extends DiskMemoizationConfig {
    val memoizationBufferSize = 10
    val memoizationWorkDir = IOUtils.createTmpDir("memotest")
    val memoizationSerialization = chunkSerialization
  }

  val storage = new Storage { }

  val testUID = "testUID"
  def genChunks(size: Int) = LimitList.genLimitList[Vector[SEvent]](size) 

  "memoization" should {
    "ensure that results are not recomputed" in {
      withMemoizationContext { ctx =>
        @volatile var i = 0;
        check { (ll: LimitList[Vector[SEvent]]) => 
          synchronized { i += 1 } 
          val events: List[Vector[SEvent]] = ll.values
          val enum = enumPStream[X, Vector[SEvent], IO](events.toStream)

          ctx.memoizing[X, Vector[SEvent]](i) must beLike {
            case Left(f) => 
              val results: List[Vector[SEvent]] = (f(consume[X, Vector[SEvent], IO, List]) &= enum[IO]).run(_ => sys.error("")).unsafePerformIO

              results must_== events and {
                ctx.memoizing[X, Vector[SEvent]](i) must beLike {
                  case Right(d) => 
                    val results2: List[Vector[SEvent]] = (consume[X, Vector[SEvent], IO, List] &= d[IO]).run(_ => sys.error("")).unsafePerformIO
                    results2 must_== events
                }
              }
          }
        }
      }
    }
  }
}


// vim: set ts=4 sw=4 et:
