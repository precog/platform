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
