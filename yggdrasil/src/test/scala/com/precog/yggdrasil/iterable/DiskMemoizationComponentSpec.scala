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
package iterable

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
import scalaz._
import scalaz.Id._
import scalaz.effect._
import scalaz.std.list._

import org.specs2.mutable._
import org.specs2.ScalaCheck
import org.scalacheck.Pretty
import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import org.scalacheck.Gen._

class DiskMemoizationComponentSpec extends Specification with DiskMemoizationComponent with StubStorageModule[Id] with ScalaCheck with ArbitrarySValue {
  override val defaultPrettyParams = Pretty.Params(2)
  type TestDataset = IterableDataset[Seq[CValue]]

  implicit val M = Monad[Id]
  implicit val actorSystem: ActorSystem = ActorSystem("leveldbMemoizationSpec")
  implicit val asyncContext = ExecutionContext.defaultExecutionContext(actorSystem)

  implicit val timeout = Timeout(intToDurationInt(30).seconds)
  implicit object memoSerialization extends IncrementalSerialization[SEvent] with SEventRunlengthFormatting with BinarySValueFormatting with ZippedStreamSerialization

  def dataset(idCount: Int, data: Iterable[(Identities, Seq[CValue])]) = IterableDataset(idCount, data)

  def sampleSize = 50

  type YggConfig = DiskMemoizationConfig 
  object yggConfig extends DiskMemoizationConfig {
    val memoizationBufferSize = 10
    val memoizationWorkDir = IOUtils.createTmpDir("memotest").unsafePerformIO
    val sortBufferSize = 10
    val sortWorkDir = IOUtils.createTmpDir("sorttest").unsafePerformIO
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
          synchronized { i += 1 } 

          var increments = 0
          val events = new Iterable[SEvent] {
            def iterator = new Iterator[SEvent] {
              private val inner = sample.value.iterator
              def hasNext = inner.hasNext 
              def next = {
                increments += 1
                inner.next
              }
            }
          }

          val result = ctx.memoize(events, i).toList
          (result must_== sample.value) and
          (increments must_== sample.value.size) and
          (ctx.memoize(events, i).toList must beLike {
            case results2 => (results2 must_== sample.value) and (increments must_== sample.value.size)
          })
        }
      }
    }.pendingUntilFixed
  }
}


// vim: set ts=4 sw=4 et:
