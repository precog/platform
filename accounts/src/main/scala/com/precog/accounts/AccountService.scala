package com.precog.accounts

import com.precog.util._
import com.precog.common.accounts._

import blueeyes._
import blueeyes.bkka._
import blueeyes.core.data._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.core.service.engines.HttpClientXLightWeb
import blueeyes.json._
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.bkka.Stoppable
import blueeyes.health.metrics.{eternity}
import blueeyes.util.Clock
import DefaultBijections._
import ByteChunk._
import HttpHeaders.Authorization

import akka.dispatch.Future
import akka.dispatch.ExecutionContext

import org.joda.time.DateTime
import org.I0Itec.zkclient.ZkClient 
import org.I0Itec.zkclient.DataUpdater
import org.streum.configrity.Configuration

import com.weiglewilczek.slf4s.Logging

import scalaz._
import scalaz.syntax.std.option._

@deprecated("TODO: This class needs badly to be replaced by a facade for the raw service.", since = "2012-12-26")
case class SecurityService(protocol: String, host: String, port: Int, path: String, rootKey: String)(implicit executor: ExecutionContext) {
  def withClient[A](f: HttpClient[ByteChunk] => A): A = {
    val client = new HttpClientXLightWeb 
    f(client.protocol(protocol).host(host).port(port).path(path))
  }
  
  def withRootClient[A](f: HttpClient[ByteChunk] => A): A = {
    val client = new HttpClientXLightWeb 
    f(client.protocol(protocol).host(host).port(port).path(path).query("apiKey", rootKey))
  }
}

trait AuthenticationCombinators extends HttpRequestHandlerCombinators {
  def auth[A](accountManager: AccountManager[Future])(service: HttpService[A, Account => Future[HttpResponse[JValue]]])(implicit ctx: ExecutionContext) = {
    new AuthenticationService[A, HttpResponse[JValue]](accountManager, service)({
      case NotProvided => HttpResponse(Unauthorized, headers = HttpHeaders(List(("WWW-Authenticate","Basic"))))
      case AuthMismatch(message) => HttpResponse(Unauthorized, content = Some(message.serialize))
    })
  }
}


trait AccountService extends BlueEyesServiceBuilder with AuthenticationCombinators {
  type AM <: AccountManager[Future]
  case class State(accountManagement: AM, stop: Stop[AM], clock: Clock, securityService: SecurityService, rootAccountId: String)

  implicit val timeout = akka.util.Timeout(120000) //for now

  implicit def executor: ExecutionContext
  implicit def M: Monad[Future]

  def accountManager(config: Configuration): (AM, Stop[AM])

  def clock: Clock

  val AccountService = service("accounts", "1.0") {
    requestLogging(timeout) {
      healthMonitor(timeout, List(eternity)) { monitor => context =>
        startup {
          import context._

          Future {
            logger.debug("Building account service state...")
            val (accountManagement, stop) = accountManager(config)
            val securityService = SecurityService(
               config[String]("security.service.protocol", "http"),
               config[String]("security.service.host", "localhost"),
               config[Int]("security.service.port", 80),
               config[String]("security.service.path", "/security/v1/"),
               config[String]("security.rootKey")
             )

            val rootAccountId = config[String]("accounts.rootAccountId", "INVALID")

            State(accountManagement, stop, clock, securityService, rootAccountId)
          }
        } ->
        request { state =>
          jsonp[ByteChunk] {
            transcode {
              path("/accounts/") {
                post(new PostAccountHandler(state.accountManagement, state.clock, state.securityService, state.rootAccountId)) ~
                auth(state.accountManagement) {
                  get(new ListAccountsHandler(state.accountManagement, state.rootAccountId)) ~ 
                  path("'accountId") {
                    get(new GetAccountDetailsHandler(state.accountManagement)) ~ 
                    delete(new DeleteAccountHandler(state.accountManagement)) ~
                    path("/password") {
                      put(new PutAccountPasswordHandler(state.accountManagement))
                    } ~ 
                    path("/grants/") {
                      post(new CreateAccountGrantHandler(state.accountManagement, state.securityService))
                    } ~
                    path("/plan") {
                      get(new GetAccountPlanHandler(state.accountManagement)) ~
                      put(new PutAccountPlanHandler(state.accountManagement)) ~ 
                      delete(new DeleteAccountPlanHandler(state.accountManagement))
                    }
                  } 
                }
              }
            }
          }
        } ->
        stop { state => 
          implicit val stop = state.stop
          Stoppable(state.accountManagement)
        }
      }
    }
  }
}
