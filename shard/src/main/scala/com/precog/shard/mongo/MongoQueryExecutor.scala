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

import com.mongodb.Mongo

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
  with EvaluatorConfig {
    
  val maxSliceSize = config[Int]("mongo.max_slice_size", 10000)

  val idSource = new IdSource {
    private val source = new java.util.concurrent.atomic.AtomicLong
    def nextId() = source.getAndIncrement
  }
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
    //FIXME: actually build this from yggConfig
    Table.mongo = new Mongo()
    true
  }

  def shutdown() = Future {
    Table.mongo.close()
    true
  }

  def browse(userUID: String, path: Path): Future[Validation[String, JArray]] = {
    Future {
      path.elements.toList match {
        case Nil => Success(Table.mongo.getDatabaseNames.asScala.sorted.serialize.asInstanceOf[JArray])

        case dbName :: Nil => 
          val db = Table.mongo.getDB(dbName)
          Success(if (db == null) JArray(Nil) else db.getCollectionNames.asScala.toList.sorted.serialize.asInstanceOf[JArray])

        case _ => 
          Failure("MongoDB paths have the form /databaseName/collectionName; longer paths are not supported.")
      }
    }
  }

  def structure(userUID: String, path: Path): Future[Validation[String, JObject]] = {
    sys.error("todo... since mongo collections are schemaless, how can we reasonably describe their structure? samping?")
  }
}


// vim: set ts=4 sw=4 et:
