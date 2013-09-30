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
package com.precog.bifrost
package mongo

import com.precog.common._
import com.precog.common.jobs._

import com.precog.common.security._
import com.precog.common.accounts._
import com.precog.standalone._
import com.precog.yggdrasil._
import com.precog.yggdrasil.actor._
import com.precog.yggdrasil.jdbm3._
import com.precog.yggdrasil.metadata._
import com.precog.yggdrasil.serialization._
import com.precog.yggdrasil.table._
import com.precog.yggdrasil.table.mongo._
import com.precog.yggdrasil.util._
import com.precog.mimir._
import com.precog.muspelheim._
import com.precog.util.FilesystemFileOps

import com.weiglewilczek.slf4s.Logging

import blueeyes.json._
import blueeyes.json.serialization._
import DefaultSerialization._

import akka.actor.ActorSystem
import akka.dispatch._
import akka.pattern.ask
import akka.util.duration._
import akka.util.Duration
import akka.util.Timeout

import com.mongodb.{Mongo, MongoURI}

import org.streum.configrity.Configuration
import org.slf4j.{LoggerFactory, MDC}

import java.io.File
import java.nio.CharBuffer

import scalaz._
import scalaz.Validation._
import scalaz.effect.IO
import scalaz.syntax.monad._
import scalaz.syntax.bifunctor._
import scalaz.syntax.std.either._
import scala.collection.JavaConverters._

class MongoQueryExecutorConfig(val config: Configuration)
    extends StandaloneQueryExecutorConfig
    with MongoColumnarTableModuleConfig {
  val logPrefix = "mongo"

  def mongoServer: String = config[String]("mongo.server", "localhost:27017")

  def dbAuthParams = config.detach("mongo.dbAuth")

  def includeIdField: Boolean = config[Boolean]("include_ids", false)
}

object MongoQueryExecutor {
  def apply(config: Configuration, jobManager: JobManager[Future], jobActorSystem: ActorSystem)(implicit ec: ExecutionContext, M: Monad[Future]): Platform[Future, StreamT[Future, Slice]] = {
    new MongoQueryExecutor(new MongoQueryExecutorConfig(config), jobManager, jobActorSystem)
  }
}

class MongoQueryExecutor(val yggConfig: MongoQueryExecutorConfig, val jobManager: JobManager[Future], val jobActorSystem: ActorSystem)(implicit val executionContext: ExecutionContext, val M: Monad[Future])
    extends StandaloneQueryExecutor
    with MongoColumnarTableModule
    with Logging { platform =>
  type YggConfig = MongoQueryExecutorConfig

  val includeIdField = yggConfig.includeIdField

  trait TableCompanion extends MongoColumnarTableCompanion
  object Table extends TableCompanion {
    var mongo: Mongo = _
    val dbAuthParams = yggConfig.dbAuthParams.data
  }

  lazy val storage = new MongoStorageMetadataSource(Table.mongo)

  def userMetadataView(apiKey: APIKey) = storage.userMetadataView(apiKey)

  Table.mongo = new Mongo(new MongoURI(yggConfig.mongoServer))

  def shutdown() = Future {
    Table.mongo.close()
    true
  }

  val metadataClient = new MetadataClient[Future] {
    def size(userUID: String, path: Path): Future[Validation[String, JNum]] = Future {
      path.elements.toList match {
        case dbName :: collectionName :: Nil =>
          val db = Table.mongo.getDB(dbName)
          success(JNum(db.getCollection(collectionName).getStats.getLong("count")))

        case _ =>
          success(JNum(0))
      }
    }.onFailure {
      case t => logger.error("Failure during size", t)
    }

    def browse(userUID: String, path: Path): Future[Validation[String, JArray]] =
      Future {
        path.elements.toList match {
          case Nil =>
            val dbs = Table.mongo.getDatabaseNames.asScala.toList
            // TODO: Poor behavior on Mongo's part, returning database+collection names
            // See https://groups.google.com/forum/#!topic/mongodb-user/HbE5wNOfl6k for details

            val finalNames = dbs.foldLeft(dbs.toSet) {
              case (acc, dbName) => acc.filterNot { t => t.startsWith(dbName) && t != dbName }
            }.toList.sorted
            Success(finalNames.map {d => d + "/" }.serialize.asInstanceOf[JArray])

          case dbName :: Nil =>
            val db = Table.mongo.getDB(dbName)
            Success(if (db == null) JArray(Nil) else db.getCollectionNames.asScala.map {d => d + "/" }.toList.sorted.serialize.asInstanceOf[JArray])

          case dbName :: collectionName :: Nil =>
            Success(JArray(Nil))

          case _ =>
            Failure("MongoDB paths have the form /databaseName/collectionName; longer paths are not supported.")
        }
      }.onFailure {
        case t => logger.error("Failure during browse", t)
      }

    def structure(userUID: String, path: Path, cpath: CPath): Future[Validation[String, JObject]] = Promise.successful (
      Success(JObject(Map("children" -> JArray.empty, "types" -> JObject.empty))) // TODO: How to implement this?
    )

    def currentVersion(apiKey: APIKey, path: Path) = Promise.successful(None)
    def currentAuthorities(apiKey: APIKey, path: Path) = Promise.successful(None)
  }
}

// vim: set ts=4 sw=4 et:
