package com.precog.shard
package mongo

import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL
import blueeyes.json.serialization._
import DefaultSerialization._

import com.precog.common.json._
import com.precog.common.security._

import com.precog.yggdrasil._
import com.precog.yggdrasil.actor._
import com.precog.yggdrasil.jdbm3._
import com.precog.yggdrasil.metadata._
import com.precog.yggdrasil.serialization._
import com.precog.yggdrasil.table._
import com.precog.yggdrasil.table.mongo._
import com.precog.yggdrasil.util._

import com.precog.daze._
import com.precog.muspelheim.ParseEvalStack

import com.precog.common._
import com.precog.util.FilesystemFileOps

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
  extends BaseConfig
  with ColumnarTableModuleConfig
  with MongoColumnarTableModuleConfig
  with BlockStoreColumnarTableModuleConfig
  with IdSourceConfig
  with EvaluatorConfig 
  with ShardConfig {
    
  val maxSliceSize = config[Int]("mongo.max_slice_size", 10000)

  val shardId = "standalone"
  val logPrefix = "mongo"

  // Ingest for mongo is handled via mongo
  override val ingestEnabled = false

  val idSource = new IdSource {
    private val source = new java.util.concurrent.atomic.AtomicLong
    def nextId() = source.getAndIncrement
  }

  def mongoServer: String = config[String]("mongo.server", "localhost:27017")

  def masterAPIKey: String = config[String]("masterAccount.apiKey", "12345678-9101-1121-3141-516171819202")
}

trait MongoQueryExecutorComponent {
  def queryExecutorFactory(config: Configuration, extAccessControl: AccessControl[Future]): QueryExecutor[Future] = {
    new MongoQueryExecutor(new MongoQueryExecutorConfig(config))
  }
}

class MongoQueryExecutor(val yggConfig: MongoQueryExecutorConfig) extends ShardQueryExecutor with MongoColumnarTableModule {
  type YggConfig = MongoQueryExecutorConfig

  val actorSystem = ActorSystem("mongoExecutorActorSystem")
  implicit val asyncContext = ExecutionContext.defaultExecutionContext(actorSystem)
  implicit val M: Monad[Future] = new blueeyes.bkka.FutureMonad(asyncContext)


  trait TableCompanion extends MongoColumnarTableCompanion
  object Table extends TableCompanion {
    var mongo: Mongo = _
  }

  def startup() = Future {
    Table.mongo = new Mongo(new MongoURI(yggConfig.mongoServer))
    true
  }

  def shutdown() = Future {
    Table.mongo.close()
    true
  }

  def browse(userUID: String, path: Path): Future[Validation[String, JArray]] = {
    Future {
      path.elements.toList match {
        case Nil => 
          val dbs = Table.mongo.getDatabaseNames.asScala.toList
          // TODO: Poor behavior on Mongo's part, returning database+collection names
          // See https://groups.google.com/forum/#!topic/mongodb-user/HbE5wNOfl6k for details
          
          val finalNames = dbs.foldLeft(dbs.toSet) {
            case (acc, dbName) => acc.filterNot { t => t.startsWith(dbName) && t != dbName }
          }.toList.sorted
          println("Final DB names = " + finalNames)
          Success(finalNames.map {d => "/" + d + "/" }.serialize.asInstanceOf[JArray])

        case dbName :: Nil => 
          val db = Table.mongo.getDB(dbName)
          Success(if (db == null) JArray(Nil) else db.getCollectionNames.asScala.map {d => "/" + d + "/" }.toList.sorted.serialize.asInstanceOf[JArray])

        case _ => 
          Failure("MongoDB paths have the form /databaseName/collectionName; longer paths are not supported.")
      }
    }
  }

  def structure(userUID: String, path: Path): Future[Validation[String, JObject]] = Future {
    Success(JObject.empty) // TODO: Implement somehow?
  }
}


// vim: set ts=4 sw=4 et:
