package com.precog.shard

import service._

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

import com.precog.analytics.Path
import com.precog.common._
import com.precog.ct._
import com.precog.ct.Mult._
import com.precog.ct.Mult.MDouble._

import com.precog.ingest._
import com.precog.ingest.service._
import com.precog.common.security._

import org.streum.configrity.Configuration

case class ShardState(queryExecutor: QueryExecutor, tokenManager: TokenManager, accessControl: AccessControl, usageLogging: UsageLogging)

trait ShardService extends BlueEyesServiceBuilder with IngestServiceCombinators {
  import IngestService._
  import BijectionsChunkJson._
  import BijectionsChunkString._
  import BijectionsChunkFutureJson._

  implicit val timeout = akka.util.Timeout(120000) //for now

  def queryExecutorFactory(config: Configuration): QueryExecutor

  def usageLoggingFactory(config: Configuration): UsageLogging 

  def tokenManagerFactory(config: Configuration): TokenManager

  val analyticsService = this.service("ingest", "1.0") {
    requestLogging(timeout) {
      healthMonitor(timeout, List(eternity)) { monitor => context =>
        startup {
          import context._

          val queryExecutor = queryExecutorFactory(config.detach("query_executor"))

          val theTokenManager = tokenManagerFactory(config.detach("security"))

          val accessControl = new TokenBasedAccessControl {
            val tokenManager = theTokenManager
          }

          queryExecutor.startup.map { _ =>
            ShardState(
              queryExecutor,
              theTokenManager,
              accessControl,
              usageLoggingFactory(config.detach("usageLogging"))
            )
          }
        } ->
        request { (state: ShardState) =>
          jsonp[ByteChunk] {
            token(state.tokenManager) {
              path("/query") {
                post(new QueryServiceHandler(state.queryExecutor))
              }
            }
          }
        } ->
        shutdown { state => Future[Option[Stoppable]]( None ) }
      }
    }
  }
}

object ShardService extends HttpRequestHandlerCombinators with PartialFunctionCombinators {
  def parsePathInt(name: String) = 
    ((err: NumberFormatException) => DispatchError(BadRequest, "Illegal value for path parameter " + name + ": " + err.getMessage)) <-: (_: String).parseInt

  def validated[A](v: Option[ValidationNEL[String, A]]): Option[A] = v map {
    case Success(a) => a
    case Failure(t) => throw new HttpException(BadRequest, t.list.mkString("; "))
  }

  def vtry[A](value: => A): Validation[Throwable, A] = try { value.success } catch { case ex => ex.fail[A] }
}
