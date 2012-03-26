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
import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import org.scalacheck.Gen._

class DiskMemoizationComponentSpec extends Specification with DiskIterableDatasetMemoizationComponent with StubYggShardComponent with ScalaCheck with ArbitrarySValue {
  override type Dataset[E] = IterableDataset[E]
  implicit val actorSystem: ActorSystem = ActorSystem("leveldb_memoization_spec")
  implicit val asyncContext = ExecutionContext.defaultExecutionContext(actorSystem)

  implicit val timeout = Timeout(intToDurationInt(30).seconds)
  implicit object memoSerialization extends IncrementalSerialization[SEvent] with SEventRunlengthFormatting with BinarySValueFormatting with ZippedStreamSerialization
  def dataset(idCount: Int, data: Iterable[(Identities, Seq[CValue])]) = IterableDataset(idCount, data)

  def sampleSize = 50

  type YggConfig = DiskMemoizationConfig 
  object yggConfig extends DiskMemoizationConfig {
    val memoizationBufferSize = 10
    val memoizationWorkDir = IOUtils.createTmpDir("memotest")
  }

  val storage = new Storage { }
  case class Unshrinkable[A](value: A)
  implicit val SEventGen = Arbitrary(listOf(sevent(3, 3)).map(Unshrinkable(_)))

  val testUID = "testUID"
  "memoization" should {
    "ensure that results are not recomputed" in {
      withMemoizationContext { ctx =>
        @volatile var i = 0;
        check { (sample: Unshrinkable[List[SEvent]]) => 
          val events = sample.value
          synchronized { i += 1 } 

          ctx.memoizing[SValue](i) must beLike {
            case Left(f) => 
              val results = Await.result(f(IterableDataset(1, events)), timeout.duration)

              (results.iterable.toList must_== events) and {
                ctx.memoizing[SValue](i) must beLike {
                  case Right(d) => 
                    val results2 = Await.result(d, timeout.duration)
                    results2.iterable.toList must_== events
                }
              }
          }
        }
      }
    }
  }
}


// vim: set ts=4 sw=4 et:
