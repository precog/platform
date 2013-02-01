package com.precog.yggdrasil
package table
package mongo

import akka.dispatch.{Await, Future, Promise}
import akka.util.Duration

import blueeyes.json.{JArray, JObject, JParser}
import blueeyes.persistence.mongo.RealMongoSpecSupport
import blueeyes.persistence.mongo.json.BijectionsMongoJson.JsonToMongo

import com.mongodb.WriteConcern
import com.mongodb.Mongo

import com.precog.bytecode._
import com.precog.common._
import com.precog.common.json._
import com.precog.common.security._
import com.precog.daze._
import com.precog.muspelheim._
import com.precog.yggdrasil.actor.StandaloneShardSystemConfig
import com.precog.yggdrasil.util._
import com.precog.util.PrecogUnit

import com.weiglewilczek.slf4s.Logging

import java.io.File

import org.specs2.specification.{Fragments, Step}

import scalaz._

object MongoPlatformSpecEngine extends Logging {
  private[this] val lock = new Object

  private[this] var engine: RealMongoSpecSupport = _
  private[this] var refcount = 0

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run() { if (engine != null) engine.shutdown() }
  })

  def acquire = lock.synchronized {
    refcount += 1

    if (engine == null) {
      logger.debug("Allocating new Mongo engine")
      engine = new RealMongoSpecSupport {}
      engine.startup()
      runLoads()
      logger.debug("Mongo engine startup complete")
    }

    logger.debug("Mongo acquired, refcount = " + refcount)

    engine
  }

  def release: Unit = lock.synchronized {
    refcount -= 1

    if (refcount == 0) {
      logger.debug("Running shutdown after final Mongo release")
      val current = engine
      engine = null
      current.shutdown()
      logger.debug("Mongo shutdown complete")
    }

    logger.debug("Mongo released, refcount = " + refcount)
  }

  def runLoads(): Unit = {
    logger.debug("Starting load")

    // Load the datasets into our test mongo by enumerating the test_data directory and loading each dataset found
    val dataDirURL = this.getClass.getClassLoader.getResource("test_data")

    if (dataDirURL == null || dataDirURL.getProtocol != "file") {
      logger.error("No data dir: " + dataDirURL)
      throw new Exception("Failed to locate test_data directory. Found: " + dataDirURL)
    }

    logger.debug("Loading from " + dataDirURL)

    val db = engine.realMongo.getDB("test")

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
            JParser.parseManyFromFile(file) match {
              case Success(data) =>
                val objs = data.map { jv =>
                  JsonToMongo.apply(jv.asInstanceOf[JObject]).fold(e => throw new Exception(e.toString), s => s)
                }.toArray
                collection.insert(objs, WriteConcern.FSYNC_SAFE)

                // Verify that things did actually make it to disk
                assert(collection.count() == objs.size)
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

trait MongoPlatformSpecs extends ParseEvalStackSpecs[Future]
    with MongoColumnarTableModule
    with Logging
    with StringIdMemoryDatasetConsumer[Future]
{ self =>

  class YggConfig extends ParseEvalStackSpecConfig
      with IdSourceConfig
      with EvaluatorConfig
      with ColumnarTableModuleConfig
      with BlockStoreColumnarTableModuleConfig
      with MongoColumnarTableModuleConfig

  object yggConfig extends YggConfig

  override def controlTimeout = Duration(10, "minutes")      // it's just unreasonable to run tests longer than this

  def includeIdField = false

  implicit val M: Monad[Future] with Copointed[Future] = new blueeyes.bkka.FutureMonad(asyncContext) with Copointed[Future] {
    def copoint[A](f: Future[A]) = Await.result(f, yggConfig.maxEvalDuration)
  }

  val report = new LoggingQueryLogger[Future, instructions.Line] with ExceptionQueryLogger[Future, instructions.Line] {
    implicit def M = self.M
  }

  trait TableCompanion extends MongoColumnarTableCompanion

  var mongo: Mongo = _

  object Table extends TableCompanion {
    import trans._

    def dbAuthParams = Map.empty
    def mongo = self.mongo
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

  class Storage extends StorageLike[Future] {
    def storeBatch(msgs: Seq[EventMessage]) = Promise.successful(PrecogUnit)
    def userMetadataView(apiKey: APIKey) = null
  }
  
  val storage = new Storage

  def userMetadataView(apiKey: APIKey) = null

  def startup() {
    mongo = MongoPlatformSpecEngine.acquire.realMongo
  }

  def shutdown() {
    MongoPlatformSpecEngine.release
  }

  override def map (fs: => Fragments): Fragments = (Step { startup() }) ^ fs ^ (Step { shutdown() })

  def Evaluator[N[+_]](N0: Monad[N])(implicit mn: Future ~> N, nm: N ~> Future) = 
    new Evaluator[N](N0)(mn,nm) with IdSourceScannerModule {
      val report = new LoggingQueryLogger[N, instructions.Line] with ExceptionQueryLogger[N, instructions.Line] {
        val M = N0
      }
      class YggConfig extends EvaluatorConfig {
        val idSource = new FreshAtomicIdSource
        val maxSliceSize = 10
      }
      val yggConfig = new YggConfig
    }
}

class MongoBasicValidationSpecs extends BasicValidationSpecs with MongoPlatformSpecs

class MongoHelloQuirrelSpecs extends HelloQuirrelSpecs with MongoPlatformSpecs

class MongoLogisticRegressionSpecs extends LogisticRegressionSpecs with MongoPlatformSpecs

class MongoMiscStackSpecs extends MiscStackSpecs with MongoPlatformSpecs

class MongoRankSpecs extends RankSpecs with MongoPlatformSpecs

class MongoRenderStackSpecs extends RenderStackSpecs with MongoPlatformSpecs

class MongoUndefinedLiteralSpecs extends UndefinedLiteralSpecs with MongoPlatformSpecs

class MongoIdFieldSpecs extends MongoPlatformSpecs {
  override def includeIdField = true

  "Mongo's _id field" should {
    "be included in results when configured" in {
      val input = """
          | campaigns := //campaigns 
          | campaigns._id""".stripMargin

      val results = evalE(input)

      results must not(beEmpty)
    }
  }
}

