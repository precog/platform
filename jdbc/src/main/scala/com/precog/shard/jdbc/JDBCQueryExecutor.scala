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
import com.precog.common.json._
import com.precog.common.accounts._
import com.precog.common.security._
import com.precog.daze._
import com.precog.muspelheim._
import com.precog.yggdrasil._
import com.precog.yggdrasil.actor._
import com.precog.yggdrasil.jdbm3._
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
import java.nio.CharBuffer
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
  extends BaseConfig
  with ColumnarTableModuleConfig
  with JDBCColumnarTableModuleConfig
  with BlockStoreColumnarTableModuleConfig
  with ShardQueryExecutorConfig
  with IdSourceConfig
  with ShardConfig {
    
  val maxSliceSize = config[Int]("mongo.max_slice_size", 10000)
  val smallSliceSize = config[Int]("mongo.small_slice_size", 8)

  val shardId = "standalone"
  val logPrefix = "jdbc"

  val idSource = new FreshAtomicIdSource

  val dbMap = config.detach("databases").data

  def masterAPIKey: String = config[String]("masterAccount.apiKey", "12345678-9101-1121-3141-516171819202")

  val clock = blueeyes.util.Clock.System

  val ingestConfig = None
}

object JDBCQueryExecutor {
  def apply(config: Configuration)(implicit ec: ExecutionContext, M: Monad[Future]): Platform[Future, StreamT[Future, CharBuffer]] = {
    new JDBCQueryExecutor(new JDBCQueryExecutorConfig(config))
  }  
}

class JDBCQueryExecutor(val yggConfig: JDBCQueryExecutorConfig)(implicit extAsyncContext: ExecutionContext, extM: Monad[Future])
    extends ShardQueryExecutorPlatform[Future] 
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

  // to satisfy abstract defines in parent traits
  val asyncContext = extAsyncContext
  val M = extM

  def startup() = Promise.successful(true)
  def shutdown() = Promise.successful(true)

  implicit val nt = NaturalTransformation.refl[Future]
  object executor extends ShardQueryExecutor[Future](M) with IdSourceScannerModule {
    val M = platform.M
    type YggConfig = platform.YggConfig
    val yggConfig = platform.yggConfig
    val report = LoggingQueryLogger[Future, instructions.Line](M)
  }

  def executorFor(apiKey: APIKey): Future[Validation[String, QueryExecutor[Future, StreamT[Future, CharBuffer]]]] = {
    Future(Success(executor))
  }

  def Evaluator[N[+_]](N0: Monad[N])(implicit mn: Future ~> N, nm: N ~> Future): EvaluatorLike[N] = {
    new Evaluator[N](N0) with IdSourceScannerModule {
      type YggConfig = platform.YggConfig // JDBMQueryExecutorConfig
      val yggConfig = platform.yggConfig
      val report = LoggingQueryLogger[N, instructions.Line](N0)
    }
  }

  val metadataClient = new MetadataClient[Future] {
    def browse(userUID: String, path: Path): Future[Validation[String, JArray]] = {
      Future {
        path.elements.toList match {
          case Nil =>
            Success(yggConfig.dbMap.keys.toList.map { d => "/" + d + "/" }.serialize.asInstanceOf[JArray])

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
                    tables ::= "/" + results.getString("TABLE_NAME") + "/"
                  }

                  tables.serialize.asInstanceOf[JArray]
                } finally {
                  conn.close()
                }
              }.bimap({ t => logger.error("Error enumerating tables", t); t.getMessage }, x => x)
            }

          case _ =>
            Failure("JDBC paths have the form /databaseName/tableName; longer paths are not supported.")
        }
      }
    }

    def structure(userUID: String, path: Path): Future[Validation[String, JObject]] = Promise.successful (
      Success(JObject.empty) // TODO: Implement from table metadata
    )
  }
}


// vim: set ts=4 sw=4 et:
