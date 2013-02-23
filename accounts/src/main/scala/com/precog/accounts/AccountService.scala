package com.precog.accounts

import com.precog.util._
import com.precog.common.NetUtils
import com.precog.common.accounts._
import com.precog.common.security._
import com.precog.common.security.service._

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

trait AuthenticationCombinators extends HttpRequestHandlerCombinators {
  def auth[A](accountManager: AccountManager[Future])(service: HttpService[A, Account => Future[HttpResponse[JValue]]])(implicit ctx: ExecutionContext) = {
    new AuthenticationService[A, HttpResponse[JValue]](accountManager, service)({
      case NotProvided => HttpResponse(Unauthorized, headers = HttpHeaders(List(("WWW-Authenticate","Basic"))))
      case AuthMismatch(message) => HttpResponse(Unauthorized, content = Some(message.serialize))
    })
  }

  sealed trait AuthenticationFailure
  case object NotProvided extends AuthenticationFailure
  case class AuthMismatch(message: String) extends AuthenticationFailure

  class AuthenticationService[A, B](accountManager: AccountManager[Future], val delegate: HttpService[A, Account => Future[B]])(err: AuthenticationFailure => B)(implicit executor: ExecutionContext)
      extends DelegatingService[A, Future[B], A, Account => Future[B]] with Logging {
    private implicit val M = new FutureMonad(executor)
    val service = (request: HttpRequest[A]) => {
      logger.info("Got authentication request " + request)
      delegate.service(request) map { (f: Account => Future[B]) =>
        request.headers.header[Authorization] flatMap {
          _.basic map {
            case BasicAuthCredentials(email,  password) =>
              accountManager.authAccount(email, password) flatMap {
                case Success(account)   => f(account)
                case Failure(error)     =>
                  logger.warn("Authentication failure from %s for %s: %s".format(NetUtils.remoteIpFrom(request), email, error))
                  Future(err(AuthMismatch("Credentials provided were formatted correctly, but did not match a known account.")))
              }
          }
        } getOrElse {
          Future(err(NotProvided))
        }
      }
    }

    val metadata = Some(AboutMetadata(ParameterMetadata('accountId, None), DescriptionMetadata("A accountId is required for the use of this service.")))
  }
}

trait AccountService extends BlueEyesServiceBuilder with AuthenticationCombinators with Logging { self =>
  case class State(handlers: AccountServiceHandlers, stop: Stoppable)

  implicit val timeout = akka.util.Timeout(120000) //for now

  implicit def executionContext: ExecutionContext
  implicit def M: Monad[Future]

  def AccountManager(config: Configuration): (AccountManager[Future], Stoppable)
  def APIKeyFinder(config: Configuration): (APIKeyManager[Future], Stoppable)

  def clock: Clock

  val AccountService = service("accounts", "1.0") {
    requestLogging(timeout) {
      healthMonitor(timeout, List(eternity)) { monitor => context =>
        startup {
          import context._

          Future {
            logger.debug("Building account service state...")
            val (accountManager, stoppable) = AccountManager(config)
            //val apiKeyFinder = APIKeyFinder(config.detach("security"))
            val (apiKeyManager, stoppable2) = APIKeyFinder(config.detach("security"))
            val apiKeyFinder = new DirectAPIKeyFinder(apiKeyManager)
            val rootAccountId = config[String]("accounts.rootAccountId", "INVALID")
            val handlers = new AccountServiceHandlers(accountManager, apiKeyFinder, clock, rootAccountId)

            State(handlers, stoppable.append(stoppable2))
          }
        } ->
        request { case State(handlers, _) =>
          import handlers._
          jsonp[ByteChunk] {
            transcode {
              path("/accounts/") {
                post(PostAccountHandler) ~
                auth(handlers.accountManager) {
                  get(ListAccountsHandler) ~
                  path("'accountId") {
                    get(GetAccountDetailsHandler) ~
                    delete(DeleteAccountHandler) ~
                    path("/password") {
                      put(PutAccountPasswordHandler)
                    } ~
                    path("/grants/") {
                      post(CreateAccountGrantHandler)
                    } ~
                    path("/plan") {
                      get(GetAccountPlanHandler) ~
                      put(PutAccountPlanHandler) ~
                      delete(DeleteAccountPlanHandler)
                    }
                  }
                }
              }
            }
          } ~
          orFail { req: HttpRequest[ByteChunk] =>
            self.logger.error("Request " + req + " could not be serviced.")
            (HttpStatusCodes.NotFound, "Request " + req + " could not be serviced.")
          }
        } ->
        stop { s: State =>
          s.stop
        }
      }
    }
  }
}
