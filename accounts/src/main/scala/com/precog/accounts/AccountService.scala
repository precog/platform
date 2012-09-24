package com.precog.accounts

import com.precog.util._
import blueeyes.BlueEyesServer

import blueeyes._
import blueeyes.core.data.{BijectionsChunkJson, BijectionsChunkFutureJson, BijectionsChunkString, ByteChunk}
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.core.service.engines.HttpClientXLightWeb
import blueeyes.json.JsonAST._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.bkka.AkkaDefaults
import blueeyes.bkka.Stoppable
import blueeyes.health.metrics.{eternity}
import blueeyes.util.Clock

import HttpHeaders.Authorization

import akka.dispatch.Future
import akka.dispatch.MessageDispatcher

import org.joda.time.DateTime
import org.I0Itec.zkclient.ZkClient 
import org.I0Itec.zkclient.DataUpdater
import org.streum.configrity.Configuration

import com.weiglewilczek.slf4s.Logging

import scalaz._
import scalaz.syntax.std.option._


case class SecurityService(protocol: String, host: String, port: Int, path: String, rootKey: String) {
  def withClient[A](f: HttpClient[ByteChunk] => A): A = {
    val client = new HttpClientXLightWeb 
    f(client.protocol(protocol).host(host).port(port).path(path))
  }
  
  def withRootClient[A](f: HttpClient[ByteChunk] => A): A = {
    val client = new HttpClientXLightWeb 
    f(client.protocol(protocol).host(host).port(port).path(path).query("apiKey", rootKey))
  }
}

case class AccountServiceState(accountManagement: AccountManagement, clock: Clock, securityService: SecurityService)


trait AccountServiceCombinators extends HttpRequestHandlerCombinators {
  implicit val jsonErrorTransform = (failure: HttpFailure, s: String) => HttpResponse(failure, content = Some(s.serialize))
  
  def auth[A, B](accountManager: AccountManager[Future])(service: HttpService[A, Account => Future[B]])(implicit err: (HttpFailure, String) => B, dispatcher: MessageDispatcher) = {
    new AuthenticationService[A, B](accountManager, service)
  }
}


trait AccountService extends BlueEyesServiceBuilder with AkkaDefaults with AccountServiceCombinators {
  import BijectionsChunkJson._
  import BijectionsChunkString._
  import BijectionsChunkFutureJson._

  implicit val timeout = akka.util.Timeout(120000) //for now

  def accountManager(config: Configuration): AccountManager[Future]

  def clock: Clock

  val AccountService = service("accounts", "1.0") {
    requestLogging(timeout) {
      healthMonitor(timeout, List(eternity)) { monitor => context =>
        startup {
          import context._

          Future {
            logger.debug("Building account service state...")
            val accountManagement = new AccountManagement(accountManager(config)) 
            val securityService = SecurityService(
               config[String]("security.service.protocol", "http"),
               config[String]("security.service.host", "localhost"),
               config[Int]("security.service.port", 80),
               config[String]("security.service.path", "/security/v1/"),
               config[String]("security.rootKey")
             )

            AccountServiceState(accountManagement, clock, securityService)
          }
        } ->
        request { (state: AccountServiceState) =>
          jsonp[ByteChunk] {
            path("/") {
              post(new PostAccountHandler(state.accountManagement, state.clock, state.securityService)) ~
              auth(state.accountManagement.accountManager) {
                get(new ListAccountsHandler(state.accountManagement)) ~ 
                path("'accountId") {
                  get(new GetAccountDetailsHandler(state.accountManagement)) ~ 
                  delete(new DeleteAccountHandler(state.accountManagement)) ~
                  path("/grants") {
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
        } ->
        shutdown { state => Future[Option[Stoppable]]( None ) }
      }
    }
  }
}
