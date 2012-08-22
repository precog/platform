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

/*
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
*/

// vim: set ts=4 sw=4 et:
