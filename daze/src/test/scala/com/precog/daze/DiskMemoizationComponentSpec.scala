package com.precog.daze

import akka.actor.ActorSystem
import akka.dispatch.ExecutionContext
import akka.dispatch.Await
import akka.util.Timeout
import akka.util.duration._

import blueeyes.json.JsonAST._

import com.precog.common._
import com.precog.common.util._
import com.precog.yggdrasil._
import com.precog.yggdrasil.shard._
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
import ArbitrarySValue._

class DiskMemoizationComponentSpec extends Specification with DiskMemoizationComponent with StubYggShardComponent with ScalaCheck {
  type X = Throwable
  implicit val actorSystem: ActorSystem = ActorSystem("leveldb_memoization_spec")
  implicit def asyncContext = ExecutionContext.defaultExecutionContext
  implicit val timeout = Timeout(intToDurationInt(30).seconds)
  implicit val chunkSerialization = BinaryProjectionSerialization
  def sampleSize = 50

  type YggConfig = DiskMemoizationConfig 
  object yggConfig extends DiskMemoizationConfig {
    val memoizationBufferSize = 10
    val memoizationWorkDir = IOUtils.createTmpDir("memotest")
    val memoizationSerialization = BinaryProjectionSerialization
  }

  object storage extends Storage

  val testUID = "testUID"

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
