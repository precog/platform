package com.precog.daze

import akka.actor.ActorSystem
import akka.dispatch.Future
import akka.dispatch.ExecutionContext
import akka.dispatch.Await
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
import SValue._

import java.io.File
import scalaz.effect._
import scalaz.iteratee._
import scalaz.std.AllInstances._
import Iteratee._

import scala.collection.immutable.SortedMap
import scala.collection.immutable.TreeMap
import org.specs2.mutable._

class DiskMemoizationComponentSpec extends Specification with DiskMemoizationComponent with StubYggShardComponent {
  implicit val actorSystem: ActorSystem = ActorSystem("leveldb_memoization_spec")
  implicit def asyncContext = ExecutionContext.defaultExecutionContext
  implicit val timeout = Timeout(intToDurationInt(30).seconds)
  implicit val chunkSerialization = SimpleProjectionSerialization
  def sampleSize = 50

  type YggConfig = DiskMemoizationConfig 
  object yggConfig extends DiskMemoizationConfig {
    val memoizationBufferSize = 10
    val memoizationWorkDir = new File("/tmp")
    val memoizationSerialization = SimpleProjectionSerialization
  }

  object storage extends Storage

  val testUID = "testUID"

  "memoization" should {
    "ensure that results are not recomputed" in {
      withMemoizationContext { ctx =>
        val (descriptor, projection) = Await.result(
          for {
            descriptors <- storage.userMetadataView(testUID).findProjections(dataPath, JPath(".cpm"))
            val descriptor = descriptors.toSeq.head._1
            projection <- storage.projection(descriptor)
          } yield {
            (descriptor, projection)
          },
          intToDurationInt(30).seconds
        )

        val expected = storage.sampleData.map(_ \ "cpm") map {
          case JInt(v) => v.toLong * 2
        }

        val enum: EnumeratorP[Unit, Vector[SEvent], IO] = projection.getAllPairs[Unit] map { _ map {
          case (ids, values) => (ids, SLong(values(0).asInstanceOf[CNum].value.toLong * 2))
        } }

        ctx.memoizing[Unit, Vector[SEvent]](0) must beLike {
          case Left(f) => 
            (
              (f(consume[Unit, Vector[SEvent], IO, List]) &= enum[IO]).run(_ => sys.error("")).unsafePerformIO.flatten map {
                case (_, v) => v.mapLongOr(-1L)(identity[Long])
              } must_== expected
            ) and (
              ctx.memoizing[Unit, Vector[SEvent]](0) must beLike {
                case Right(d) => 
                  (
                    (consume[Unit, Vector[SEvent], IO, List] &= d[IO]).run(_ => sys.error("")).unsafePerformIO.flatten map {
                      //case (_, v) => v.mapBigDecimalOr(-1L)(v => v.toLong)
                      case (_, v) => v.mapLongOr(-1L)(v => v.toLong) // TODO: re-fix this for BigDecimal if that's really what it's supposed to be
                    } must_== expected
                  ) 
              }
            )
        }
      }
    }
  }
}


// vim: set ts=4 sw=4 et:
