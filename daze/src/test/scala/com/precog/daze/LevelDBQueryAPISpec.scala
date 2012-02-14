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
        case (pd, p) => ((pd.columns(0).selector, p.getAllPairs[Unit] map { case (ids, vs) =>  (ids, vs(0)) })) 
      } toList

      val enum = query.combine(projectionData) map { case (ids, sv) => sv }
      
      (consume[Unit, SValue, IO, List] &= enum[IO]).run(_ => sys.error("...")).unsafePerformIO must haveTheSameElementsAs(storage.sampleData.map(fromJValue))
    }
  }

  "fullProjection" should {
    "return all of the objects inserted into projections" in {
      val enum = Await.result(query.fullProjection[Unit](dataPath) map { case (ids, sv) => sv } fenum, intToDurationInt(30).seconds)
      
      (consume[Unit, SValue, IO, List] &= enum[IO]).run(_ => sys.error("...")).unsafePerformIO must haveTheSameElementsAs(storage.sampleData.map(fromJValue))
    }
  }

  "mask" should {
    "descend" in {
      val enum = Await.result(query.mask[Unit](dataPath).derefObject("gender").realize.fenum, intToDurationInt(30).seconds)
      val enumv = enum map { case (ids, sv) => sv }
      (consume[Unit, SValue, IO, List] &= enumv[IO]).run(_ => sys.error("...")).unsafePerformIO must haveTheSameElementsAs(storage.sampleData.map(v => fromJValue(v \ "gender")))
    }
  }
}


// vim: set ts=4 sw=4 et:
