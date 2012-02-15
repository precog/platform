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
import StorageMetadata._
import SValue._

import java.io.File
import scalaz.effect._
import scalaz.iteratee._
import scalaz.std.AllInstances._
import Iteratee._

import scala.collection.immutable.SortedMap
import scala.collection.immutable.TreeMap
import org.specs2.mutable._

class LevelDBMemoizationContextSpec extends Specification with LevelDBMemoizationComponent with StubYggShardComponent {
  implicit val actorSystem: ActorSystem = ActorSystem("leveldb_memoization_spec")
  implicit def asyncContext = ExecutionContext.defaultExecutionContext
  implicit val timeout = Timeout(intToDurationInt(30).seconds)

  type YggConfig = LevelDBMemoizationConfig 
  object yggConfig extends LevelDBMemoizationConfig {
    val memoizationBufferSize = 100
    val memoizationWorkDir = new File("")
  }

  object storage extends Storage

  "memoization" should {
    "ensure that results are not recomputed" in {
      withMemoizationContext { memoizationContext => 
        val (descriptor, projection) = Await.result(
          for {
            descriptors <- storage.metadata.findProjections(dataPath, JPath(".cpm"))
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

        val enum: EnumeratorP[Unit, SEvent, IO] = projection.getAllPairs[Unit] map { 
          case (ids, values) => (ids, SLong(values(0).asInstanceOf[CNum].value.toLong * 2))
        }

        memoizationContext[Unit](0) must beLike {
          case Left(f) => 
            (
              (f[IO, List[SEvent]](Some(descriptor)).apply(consume[Unit, SEvent, IO, List]) &= enum[IO]).run(_ => sys.error("")).unsafePerformIO map {
                case (_, v) => v.mapLongOr(-1L)(identity[Long])
              } must_== expected
            ) and (
              memoizationContext[Unit](0) must beLike {
                case Right(d) => 
                  (
                    (consume[Unit, SEvent, IO, List] &= Await.result(d.fenum, intToDurationInt(30).seconds).apply[IO]).run(_ => sys.error("")).unsafePerformIO map {
                      case (_, v) => v.mapLongOr(-1L)(identity[Long])
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
