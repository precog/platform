package com.precog.yggdrasil
package iterable

import akka.actor._
import akka.dispatch._
import akka.util.Timeout
import akka.util.duration._

import blueeyes.json.JPath
import blueeyes.json.JsonAST._
import blueeyes.json.JsonParser

import com.precog.common._
import com.precog.common.util._
import com.precog.yggdrasil.util._
import com.precog.util._
import SValue._

import scalaz.Id._

import java.io.File

trait StubLevelDBQueryComponent extends LevelDBQueryComponent with DistributedSampleStubStorageModule[Future] with IterableDatasetOpsComponent {
  type TestDataset = Dataset[Seq[CValue]]

  trait YggConfig extends LevelDBQueryConfig with IterableDatasetOpsConfig {
    val projectionRetrievalTimeout = Timeout(intToDurationInt(10).seconds)
    val clock = blueeyes.util.Clock.System
    val sortBufferSize: Int = 1000
    val sortWorkDir: File = new File("/tmp")
  }

  implicit val actorSystem: ActorSystem = ActorSystem("leveldb-query-api-spec")
  implicit def asyncContext = ExecutionContext.defaultExecutionContext
  implicit def M = blueeyes.bkka.AkkaTypeClasses.futureApplicative(asyncContext)

  def sampleSize = 1 

  val testUID = "testUID"
  def dataset(idCount: Int, data: Iterable[(Identities, Seq[CValue])]) = IterableDataset(idCount, data)

  object yggConfig extends YggConfig

  object query extends QueryAPI
  object ops extends Ops
}

// vim: set ts=4 sw=4 et:
