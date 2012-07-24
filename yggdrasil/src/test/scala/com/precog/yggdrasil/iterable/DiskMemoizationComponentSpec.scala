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
