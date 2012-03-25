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

import java.io.File
import scalaz.effect._
import scalaz.iteratee._
import scalaz.std.anyVal._
import scalaz.std.list._
import Iteratee._

import scala.collection.immutable.SortedMap
import scala.collection.immutable.TreeMap
import org.specs2.mutable._

class LevelDBQueryAPISpec extends Specification with LevelDBQueryComponent 
with StubYggShardComponent with IterableDatasetOpsComponent {
  type YggConfig = SortConfig with LevelDBQueryConfig
  override type Dataset[E] = IterableDataset[E]

  implicit val actorSystem: ActorSystem = ActorSystem("leveldb_query_api_spec")
  implicit def asyncContext = ExecutionContext.defaultExecutionContext
  def sampleSize = 1 

  val testUID = "testUID"

  object yggConfig extends SortConfig with LevelDBQueryConfig {
    val projectionRetrievalTimeout = Timeout(intToDurationInt(10).seconds)
    val clock = blueeyes.util.Clock.System
    val sortBufferSize: Int = 1000
    val sortWorkDir: File = new File("/tmp")
  }

  val storage = new Storage { }

  object query extends QueryAPI
  object ops extends Ops

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
