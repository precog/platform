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
package table
package mongo

import akka.dispatch.{Await, Future, Promise}
import akka.util.Duration

import blueeyes.json.{JArray, JObject, JParser}
import blueeyes.persistence.mongo.RealMongoSpecSupport
import blueeyes.persistence.mongo.json.BijectionsMongoJson.JsonToMongo

import com.mongodb.WriteConcern

import com.precog.bytecode._
import com.precog.common._
import com.precog.common.json._
import com.precog.common.security._
import com.precog.daze.StringIdMemoryDatasetConsumer
import com.precog.muspelheim._
import com.precog.yggdrasil.actor.StandaloneShardSystemConfig
import com.precog.yggdrasil.util.IdSourceConfig
import com.precog.util.PrecogUnit

import com.weiglewilczek.slf4s.Logging

import java.io.File

import scalaz._

trait MongoPlatformSpecs extends ParseEvalStackSpecs[Future]
    with MongoColumnarTableModule
    with RealMongoSpecSupport
    with Logging
    with StringIdMemoryDatasetConsumer[Future]
{ self =>

  class YggConfig extends ParseEvalStackSpecConfig
      with IdSourceConfig
      with ColumnarTableModuleConfig
      with BlockStoreColumnarTableModuleConfig
      with MongoColumnarTableModuleConfig

  object yggConfig extends YggConfig

  override def controlTimeout = Duration(10, "minutes")      // it's just unreasonable to run tests longer than this

  implicit val M: Monad[Future] with Copointed[Future] = new blueeyes.bkka.FutureMonad(asyncContext) with Copointed[Future] {
    def copoint[A](f: Future[A]) = Await.result(f, yggConfig.maxEvalDuration)
  }

  trait TableCompanion extends MongoColumnarTableCompanion

  object Table extends TableCompanion {
    import trans._

    def dbAuthParams = Map.empty
    val mongo = self.realMongo
    override def load(table: Table, apiKey: APIKey, tpe: JType): Future[Table] = {
      // Rewrite paths of the form /foo/bar/baz to /test/foo_bar_baz
      val pathFixTS = Map1(Leaf(Source), CF1P("fix_paths") {
        case orig: StrColumn => new StrColumn {
          def apply(row: Int): String = {
            val newPath = "/test/" + orig(row).replaceAll("^/|/$", "").replace('/', '_')
            logger.debug("Fixed %s to %s".format(orig(row), newPath))
            newPath
          }
          def isDefinedAt(row: Int) = orig.isDefinedAt(row)
        }
      })
      val transformed = table.transform(pathFixTS)
      super.load(transformed, apiKey, tpe)
    }
  }

  class Storage extends StorageLike {
    def projection(descriptor: ProjectionDescriptor) = Promise.successful(null) // FIXME: Just to get it compiling...
    def storeBatch(msgs: Seq[EventMessage]) = Promise.successful(PrecogUnit)
    def userMetadataView(apiKey: APIKey) = null
  }
  
  val storage = new Storage

  override def startup() {
    logger.debug("Starting mongo")
    super[RealMongoSpecSupport].startup()

    logger.debug("Starting load")

    // Load the datasets into our test mongo by enumerating the test_data directory and loading each dataset found
    val dataDirURL = this.getClass.getClassLoader.getResource("test_data")

    if (dataDirURL == null || dataDirURL.getProtocol != "file") {
      logger.error("No data dir: " + dataDirURL)
      throw new Exception("Failed to locate test_data directory. Found: " + dataDirURL)
    }

    logger.debug("Loading from " + dataDirURL)

    val db = realMongo.getDB("test")

    def loadFile(path : String, file: File) {
      if (file.isDirectory) {
        file.listFiles.foreach { f => 
          logger.debug("Found child: " + f)
          loadFile(path + file.getName + "_", f)
        }
      } else {
        if (file.getName.endsWith(".json")) {
          try {
            val collectionName = path + file.getName.replace(".json","")
            logger.debug("Loading %s into /test/%s".format(file, collectionName))
            val collection = db.getCollection(collectionName)
            JParser.parseFromFile(file) match {
              case Success(JArray(data)) =>
                val objs = data.map { jv =>
                  JsonToMongo.apply(jv.asInstanceOf[JObject]).fold(e => throw new Exception(e.toString), s => s)
                }.toArray
                collection.insert(objs, WriteConcern.FSYNC_SAFE)
              case Failure(error) => logger.error("Error loading: " + error)
            }
          } catch {
            case t: Throwable => logger.error("Error loading: " + t)
          }
        }
      }
    }

    (new File(dataDirURL.toURI)).listFiles.foreach { f =>
      loadFile("", f)
    }
  }
}

class MongoBasicValidationSpecs extends BasicValidationSpecs with MongoPlatformSpecs

class MongoHelloQuirrelSpecs extends HelloQuirrelSpecs with MongoPlatformSpecs

class MongoLogisticRegressionSpecs extends LogisticRegressionSpecs with MongoPlatformSpecs

class MongoMiscStackSpecs extends MiscStackSpecs with MongoPlatformSpecs

class MongoRankSpecs extends RankSpecs with MongoPlatformSpecs

class MongoRenderStackSpecs extends RenderStackSpecs with MongoPlatformSpecs

class MongoUndefinedLiteralSpecs extends UndefinedLiteralSpecs with MongoPlatformSpecs

