package com.precog.ingest.service

import blueeyes._
import blueeyes.bkka._
import blueeyes.core.data.{BijectionsChunkJson, BijectionsChunkFutureJson, BijectionsChunkString, ByteChunk}
import blueeyes.core.http._
import blueeyes.core.http.MimeTypes.{application, json}
import blueeyes.core.service._
import blueeyes.core.service.RestPathPattern._
import blueeyes.health.metrics.{eternity}
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json.{JPath, JsonParser, JPathField}
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultOrderings._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.JodaSerializationImplicits._
import blueeyes.persistence.mongo._
import blueeyes.persistence.cache.{Stage, ExpirationPolicy, CacheSettings}
import blueeyes.util.{Clock, ClockSystem, PartialFunctionCombinators, InstantOrdering}
import scala.math.Ordered._
import HttpStatusCodes.{BadRequest, Unauthorized, Forbidden}

import akka.dispatch.Future

import net.lag.configgy.{Configgy, ConfigMap}

import org.joda.time.base.AbstractInstant
import org.joda.time.Instant
import org.joda.time.DateTime
import org.joda.time.DateTimeZone

import java.net.URL
import java.util.concurrent.TimeUnit

import scala.collection.immutable.SortedMap
import scala.collection.immutable.IndexedSeq

import scalaz.Monoid
import scalaz.Validation
import scalaz.ValidationNEL
import scalaz.Success
import scalaz.Failure
import scalaz.NonEmptyList
import scalaz.Scalaz._

import com.precog.analytics._
import com.precog.ct._
import com.precog.ct.Mult._
import com.precog.ct.Mult.MDouble._

//import com.precog.instrumentation.blueeyes.ReportGridInstrumentation
//import com.precog.api.ReportGridTrackingClient
import com.precog.ingest.service.service._
import com.precog.ingest.service._

case class IngestState(indexMongo: Mongo, tokenManager: TokenManager, eventStore: EventStore, storageReporting: StorageReporting)

trait IngestService extends BlueEyesServiceBuilder with IngestServiceCombinators {
  import IngestService._
  import BijectionsChunkJson._
  import BijectionsChunkString._
  import BijectionsChunkFutureJson._

  implicit val timeout = akka.util.Timeout(120000) //for now

  def eventStoreFactory(configMap: ConfigMap): EventStore

  def mongoFactory(configMap: ConfigMap): Mongo

  //def auditClient(configMap: ConfigMap): ReportGridTrackingClient[JValue] 

  def storageReporting(configMap: ConfigMap): StorageReporting

  def tokenManager(database: Database, tokensCollection: MongoCollection, deletedTokensCollection: MongoCollection): TokenManager

  val clock: Clock

  val analyticsService = this.service("ingest", "1.0") {
    requestLogging(timeout) {
      healthMonitor(timeout, List(eternity)) { monitor => context =>
        startup {
          import context._

          val indexdbConfig = config.configMap("indexdb")
          val indexMongo = mongoFactory(indexdbConfig)
          val indexdb  = indexMongo.database(indexdbConfig.getString("database", "analytics-v" + serviceVersion))

          val tokensCollection = config.getString("tokens.collection", "tokens")
          val deletedTokensCollection = config.getString("tokens.deleted", "deleted_tokens")
          val tokenMgr = tokenManager(indexdb, tokensCollection, deletedTokensCollection)

          val eventStore = eventStoreFactory(config.configMap("eventStore"))

          Future(IngestState(
            indexMongo,
            tokenMgr,
            eventStore,
            storageReporting(config.configMap("storageReporting"))
            ))
        } ->
        request { (state: IngestState) =>

          //val audit = auditor(state.auditClient, clock, state.tokenManager)
          //import audit._

          jsonp[ByteChunk] {
            token(state.tokenManager) {
              /* The virtual file system, which is used for storing data,
               * retrieving data, and querying for metadata.
               */
              path("/store") {
                dataPath("vfs") {
                  post(new TrackingService(state.eventStore, state.storageReporting, clock, false))
                }
              } ~ 
              dataPath("vfs") {
                post(new TrackingService(state.eventStore, state.storageReporting, clock, true))
              } ~ 
              path("/echo") {
                dataPath("vfs") {
                  get(new EchoServiceHandler())
                }
              }
            }
          }
        } ->
        shutdown { state => 
          Future( 
            Option(
              Stoppable(state.tokenManager.database, Stoppable(state.indexMongo) :: Nil)
            )
          )
        }
      }
    }
  }
}

object IngestService extends HttpRequestHandlerCombinators with PartialFunctionCombinators {
  def parsePathInt(name: String) = 
    ((err: NumberFormatException) => DispatchError(BadRequest, "Illegal value for path parameter " + name + ": " + err.getMessage)) <-: (_: String).parseInt

  def validated[A](v: Option[ValidationNEL[String, A]]): Option[A] = v map {
    case Success(a) => a
    case Failure(t) => throw new HttpException(BadRequest, t.list.mkString("; "))
  }

  def vtry[A](value: => A): Validation[Throwable, A] = try { value.success } catch { case ex => ex.fail[A] }
}
