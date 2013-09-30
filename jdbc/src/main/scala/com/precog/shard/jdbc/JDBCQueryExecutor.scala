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
package com.precog.shard
package jdbc

import blueeyes.json._
import blueeyes.json.serialization._
import DefaultSerialization._

import com.precog.common._
import com.precog.common.jobs._

import com.precog.common.accounts._
import com.precog.common.security._
import com.precog.mimir._
import com.precog.muspelheim._
import com.precog.standalone._
import com.precog.yggdrasil._
import com.precog.yggdrasil.actor._
import com.precog.yggdrasil.metadata._
import com.precog.yggdrasil.serialization._
import com.precog.yggdrasil.table._
import com.precog.yggdrasil.table.jdbc._
import com.precog.yggdrasil.util._
import com.precog.util.FilesystemFileOps

import akka.actor.ActorSystem
import akka.dispatch._
import akka.pattern.ask
import akka.util.duration._
import akka.util.Duration
import akka.util.Timeout

import org.streum.configrity.Configuration

import com.weiglewilczek.slf4s.Logging

import java.io.File
import java.sql.DriverManager

import scalaz._
import scalaz.Validation._
import scalaz.effect.IO
import scalaz.syntax.monad._
import scalaz.syntax.bifunctor._
import scalaz.syntax.std.either._
import scalaz.syntax.std.option._
import scala.collection.JavaConverters._

class JDBCQueryExecutorConfig(val config: Configuration)
    extends StandaloneQueryExecutorConfig
    with JDBCColumnarTableModuleConfig {
  val logPrefix = "jdbc"

  val dbMap = config.detach("databases").data
}

object JDBCQueryExecutor {
  def apply(config: Configuration, jobManager: JobManager[Future], jobActorSystem: ActorSystem)(implicit ec: ExecutionContext, M: Monad[Future]): ManagedPlatform = {
    new JDBCQueryExecutor(new JDBCQueryExecutorConfig(config), jobManager, jobActorSystem)
  }
}

class JDBCQueryExecutor(val yggConfig: JDBCQueryExecutorConfig, val jobManager: JobManager[Future], val jobActorSystem: ActorSystem)(implicit val executionContext: ExecutionContext, val M: Monad[Future])
    extends StandaloneQueryExecutor
    with JDBCColumnarTableModule
    with Logging { platform =>
  type YggConfig = JDBCQueryExecutorConfig

  trait TableCompanion extends JDBCColumnarTableCompanion
  object Table extends TableCompanion {
    val databaseMap = yggConfig.dbMap
  }

  // Not for production
  val unescapeColumnNames = false

  lazy val storage = new JDBCStorageMetadataSource(yggConfig.dbMap)
  def userMetadataView(apiKey: APIKey) = storage.userMetadataView(apiKey)

  def startup() = Promise.successful(true)
  def shutdown() = Promise.successful(true)

  val metadataClient = new MetadataClient[Future] {
    def size(userUID: String, path: Path): Future[Validation[String, JNum]] = Future {
      path.elements.toList match {
        case dbName :: tableName :: Nil =>
          yggConfig.dbMap.get(dbName).toSuccess("DB %s is not configured".format(dbName)) flatMap { url =>
            Validation.fromTryCatch {
              val conn = DriverManager.getConnection(url)

              try {
                // May need refinement to get meaningful results
                val stmt = conn.createStatement

                val query = "SELECT count(*) as count FROM " + tableName.filterNot(_ == ';')
                logger.debug("Querying with " + query)

                val result = stmt.executeQuery(query)

                if (result.next) {
                  JNum(result.getLong("count"))
                } else {
                  JNum(0)
                }
              } finally {
                conn.close()
              }
            }.bimap({ t => logger.error("Error enumerating tables", t); t.getMessage }, x => x)
          }

        case _ =>
          Success(JNum(0))
      }
    }.onFailure {
      case t => logger.error("Failure during size", t)
    }

    def browse(userUID: String, path: Path): Future[Validation[String, JArray]] = {
      Future {
        path.elements.toList match {
          case Nil =>
            Success(yggConfig.dbMap.keys.toList.map { d => d + "/" }.serialize.asInstanceOf[JArray])

          case dbName :: Nil =>
            // A little more complicated. Need to use metadata interface to enumerate table names
            yggConfig.dbMap.get(dbName).toSuccess("DB %s is not configured".format(dbName)) flatMap { url =>
              Validation.fromTryCatch {
                val conn = DriverManager.getConnection(url)

                try {
                  // May need refinement to get meaningful results
                  val results = conn.getMetaData.getTables(null, null, "%", Array("TABLE"))

                  var tables = List.empty[String]

                  while (results.next) {
                    tables ::= results.getString("TABLE_NAME") + "/"
                  }

                  tables.serialize.asInstanceOf[JArray]
                } finally {
                  conn.close()
                }
              }.bimap({ t => logger.error("Error enumerating tables", t); t.getMessage }, x => x)
            }

          case dbName :: collectionName :: Nil =>
            Success(JArray(Nil))

          case _ =>
            Failure("JDBC paths have the form /databaseName/tableName; longer paths are not supported.")
        }
      }.onFailure {
        case t => logger.error("Failure during size", t)
      }
    }

    def structure(userUID: String, path: Path, cpath: CPath): Future[Validation[String, JObject]] = Promise.successful (
      Success(JObject(Map("children" -> JArray.empty, "types" -> JObject.empty))) // TODO: Implement from table metadata
    )

    def currentVersion(apiKey: APIKey, path: Path) = Promise.successful(None)
    def currentAuthorities(apiKey: APIKey, path: Path) = Promise.successful(None)
  }
}


// vim: set ts=4 sw=4 et:
