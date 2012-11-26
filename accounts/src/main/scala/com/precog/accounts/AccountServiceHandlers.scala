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
package com.precog
package accounts

import com.precog.common.Path
import com.precog.auth.WrappedAPIKey
import com.precog.common.security._

import akka.dispatch.{ ExecutionContext, Future, Await }
import akka.util.Timeout
import akka.util.duration._

import blueeyes.bkka.AkkaTypeClasses._
import blueeyes.core.data.ByteChunk
import blueeyes.core.data.DefaultBijections._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.http.MimeTypes._
import blueeyes.core.service._
import blueeyes.core.service.engines.HttpClientXLightWeb
import blueeyes.json._
import blueeyes.json.serialization.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.serialization.DefaultSerialization.{ DateTimeDecomposer => _, DateTimeExtractor => _, _ }
import blueeyes.json.serialization.Extractor._

import blueeyes.core.http.MimeTypes._
import blueeyes.core.data._
import DefaultBijections._
import blueeyes.core.service.engines.HttpClientXLightWeb

import blueeyes.util.Clock

import HttpHeaders.Authorization

import com.weiglewilczek.slf4s.Logging

import scalaz.{ Applicative, Validation, Success, Failure }

import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

object Responses {
  def failure(error: HttpStatusCode, message: String) = 
    HttpResponse[JValue](HttpStatus(error), content = Some(JString(message))) 

  def failure(error: HttpStatusCode, statMessage: String, message: String) = 
    HttpResponse[JValue](HttpStatus(error, statMessage), content = Some(JString(message)))
  
  def failure(error: HttpStatus, message: String) = 
    HttpResponse[JValue](error, content = Some(JString(message)))
}

sealed trait AuthenticationFailure
case object NotProvided extends AuthenticationFailure
case class AuthMismatch(message: String) extends AuthenticationFailure

class AuthenticationService[A, B](accountManager: AccountManager[Future], val delegate: HttpService[A, Account => Future[B]])(err: AuthenticationFailure => B)(implicit ctx: ExecutionContext) 
extends DelegatingService[A, Future[B], A, Account => Future[B]] with Logging {
  val service = (request: HttpRequest[A]) => {
    delegate.service(request) map { (f: Account => Future[B]) =>
      request.headers.header[Authorization] flatMap {
        _.basic map {
          case BasicAuthCredentials(email,  password) =>
            accountManager.authAccount(email, password) flatMap { 
              case Some(account)   => f(account)
              case None            => Future(err(AuthMismatch("Credentials provided were formatted correctly, but did not match a known account.")))
            }
        }
      } getOrElse {
        Future(err(NotProvided))
      }
    }
  }
  
  val metadata = Some(AboutMetadata(ParameterMetadata('accountId, None), DescriptionMetadata("A accountId is required for the use of this service.")))
}

trait AccountAuthorization {
  def accountManagement: AccountManager[Future]

  def withAccountAdmin[A](accountId: String, auth: Account)(f: Account => Future[HttpResponse[JValue]])(implicit executor: ExecutionContext): Future[HttpResponse[JValue]] = {
    accountManagement.findAccountById(accountId) flatMap { 
      case Some(account) =>
        accountManagement.hasAncestor(account, auth) flatMap {
          case true  => f(account)
          case false => Future(HttpResponse[JValue](HttpStatus(Unauthorized), content = Some(JString("You do not have access to account "+ accountId))))
        }

      case None => 
        Future(HttpResponse[JValue](HttpStatus(NotFound), content = Some(JString("Unable to find Account "+ accountId))))
    }
  }

  def withAccountAdmin[A](request: HttpRequest[_], auth: Account)(f: Account => Future[HttpResponse[JValue]])(implicit executor: ExecutionContext): Future[HttpResponse[JValue]] = {
    request.parameters.get('accountId).map { accountId =>
      withAccountAdmin(accountId, auth) { f }
    } getOrElse {
      Future(HttpResponse[JValue](HttpStatus(BadRequest, "Missing accountId in request URI."), content = Some(JString("Missing accountId in request URI."))))
    }
  }
}


class ListAccountsHandler(accountManagement: AccountManager[Future])(implicit ctx: ExecutionContext) 
extends CustomHttpService[Future[JValue], Account => Future[HttpResponse[JValue]]] with Logging {
  val service: HttpRequest[Future[JValue]] => Validation[NotServed,Account => Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => {
    Success { (auth: Account) =>
      //TODO, if root then can see more
      accountManagement.listAccountIds(auth.apiKey).map { 
        case accounts =>
          val objs:List[JObject] = accounts.map {
            account => JObject(Map("accountId" -> JString(account.accountId)))
          } (collection.breakOut)
          HttpResponse[JValue](OK, content = Some(JArray(objs)))
      }
    }
  }

  val metadata = None
}


//returns accountId of account if exists, else creates account, 
//we are working on path accountId.. do we use this to get the account the user wants to create?
//because we also need auth at this stage.. auth will give us the root key for permmissions
class PostAccountHandler(accountManagement: AccountManager[Future], clock: Clock, securityService: SecurityService, rootAccountId: String)(implicit ctx: ExecutionContext) 
extends CustomHttpService[Future[JValue], Future[HttpResponse[JValue]]] with Logging {
   
  val service: HttpRequest[Future[JValue]] => Validation[NotServed, Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => {
    logger.trace("Got request in PostAccountHandler: " + request)
    request.content map { futureContent => 
      Success(
        futureContent flatMap { jv =>
          (jv \ "email", jv \ "password") match {
            case (JString(email), JString(password)) =>
              logger.debug("About to create account for email " + email)
              for {
                existingAccountOpt <- accountManagement.findAccountByEmail(email)
                accountResponse <- 
                  existingAccountOpt map { account =>
                    logger.debug("Found existing account: " + account.accountId)
                    Future(HttpResponse[JValue](OK, content = Some(JObject(List(JField("accountId", account.accountId))))))
                  } getOrElse {
                    accountManagement.newAccount(email, password, clock.now(), AccountPlan.Free) { (accountId, path) =>
                      val request = NewAPIKeyRequest.newAccount(accountId, path, None, None)
                      val createBody = request.serialize 

                      logger.debug("Creating new account with id " + accountId + " and request body " + createBody)

                      securityService.withRootClient { client =>
                        client.contentType(application/MimeTypes.json).path("apikeys/").post[JValue]("")(createBody) map {
                          case HttpResponse(HttpStatus(OK, _), _, Some(wrappedKey), _) =>
                           wrappedKey.validated[WrappedAPIKey] match {
                             case Success(WrappedAPIKey(apiKey, _, _)) => apiKey
                             case Failure(err) =>
                              logger.error("Unexpected response to API key creation request: " + err)
                              throw HttpException(BadGateway, "Unexpected response to API key creation request: " + err)
                           }

                          case HttpResponse(HttpStatus(failure: HttpFailure, reason), _, content, _) => 
                            logger.error("Fatal error attempting to create api key: " + failure + ": " + content)
                            throw HttpException(failure, reason)

                          case x => 
                            logger.error("Unexpected response from api provisioning service: " + x)
                            throw HttpException(BadGateway, "Unexpected response from the api provisioning service: " + x)
                        }
                      }
                    } map { account =>
                      // todo: send email ?
                      logger.debug("Account successfully created: " + account.accountId)
                      HttpResponse[JValue](OK, content = Some(JObject(JField("accountId", account.accountId) :: Nil)))
                    }
                  }
              } yield accountResponse

            case _ =>
              val errmsg = "Missing email and/or password fiedlds from request body."
              Future(HttpResponse[JValue](HttpStatus(BadRequest, errmsg), content = Some(JString(errmsg))))
          }
        }
      )
    } getOrElse {
      Failure(DispatchError(HttpException(BadRequest, "Missing request body content.")))
    }
  }

  val metadata = None
}


class CreateAccountGrantHandler(accountManagement: AccountManager[Future], securityService: SecurityService)(implicit ctx: ExecutionContext) 
extends CustomHttpService[Future[JValue], Account =>  Future[HttpResponse[JValue]]] with Logging {
  val service: HttpRequest[Future[JValue]] => Validation[NotServed, Account => Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => {
    Success { (auth: Account) =>
      // cannot use withAccountAdmin here because of the ability to add grants to others' accounts.
      request.parameters.get('accountId) map { accountId =>
        accountManagement.findAccountById(accountId) flatMap {
          case Some(account) => 
            request.content map { futureContent => 
              futureContent flatMap { jvalue =>
                //todo: validate the jvalue, and place behind a trait for mocking?
                securityService.withClient { client =>
                  client.query("apiKey", auth.apiKey)
                        .contentType(application/MimeTypes.json)
                        .post[JValue]("apikeys/" + account.apiKey + "/grants/")(jvalue) map {
                                                    
                    case HttpResponse(HttpStatus(Created, _), _, None, _) => 
                      HttpResponse[JValue](OK, content = Some(""))
                    
                    case _ =>
                      HttpResponse[JValue](HttpStatus(InternalServerError), content = Some(JString("could not create grants")))
                  }
                }
              }
            } getOrElse {
              Future(HttpResponse(HttpStatus(BadRequest, "Missing request body."),
                                  content = Some(JString("Missing request body"))))
            }
                
          case _  => 
            Future(HttpResponse[JValue](HttpStatus(NotFound), content = Some(JString("Unable to find account "+ accountId))))
        }
      } getOrElse {
        Future(HttpResponse[JValue](HttpStatus(BadRequest, "Missing account id."), 
                                    content = Some(JString("Missing account id."))))
      }
    }
  }

  val metadata = None
}


//returns plan for account
class GetAccountPlanHandler(val accountManagement: AccountManager[Future])(implicit ctx: ExecutionContext) 
    extends CustomHttpService[Future[JValue],Account => Future[HttpResponse[JValue]]] 
    with AccountAuthorization
    with Logging {

  val service: HttpRequest[Future[JValue]] => Validation[NotServed, Account => Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => {
    Success { (auth: Account) => 
      withAccountAdmin(request, auth) { account =>
        Future(HttpResponse[JValue](OK, content = Some(JObject(List(JField("type", account.plan.planType))))))
      }
    }
  }

  val metadata = None
}


//update account password
class PutAccountPasswordHandler(val accountManagement: AccountManager[Future])(implicit ctx: ExecutionContext) 
    extends CustomHttpService[Future[JValue], Account =>Future[HttpResponse[JValue]]] 
    with AccountAuthorization
    with Logging {

  val service: HttpRequest[Future[JValue]] => Validation[NotServed, Account => Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => {
    Success { (auth: Account) =>
      withAccountAdmin(request, auth) { account =>
        request.content map { futureContent =>
          futureContent flatMap { jvalue =>
            (jvalue \ "password").validated[String] match {
              case Success(newPassword) => 
                accountManagement.updateAccountPassword(account, newPassword) map { 
                  case true => HttpResponse[JValue](OK, content = None)
                  case _ => Responses.failure(InternalServerError, "Account update failed, please contact support.")
                } 

              case Failure(error) => 
                Future(HttpResponse[JValue](HttpStatus(BadRequest, "Invalid request body."), content = Some(JString("Could not determine replacement password from request body."))))
            }
          }
        } getOrElse {
          Future(HttpResponse[JValue](HttpStatus(BadRequest, "Request body missing."), content = Some(JString("You must provide a JSON object containing a password field."))))
        }
      }
    }
  }

  val metadata = None
}


//update account Plan
class PutAccountPlanHandler(val accountManagement: AccountManager[Future])(implicit ctx: ExecutionContext) 
    extends CustomHttpService[Future[JValue], Account =>Future[HttpResponse[JValue]]] 
    with AccountAuthorization
    with Logging {

  val service: HttpRequest[Future[JValue]] => Validation[NotServed, Account => Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => {
    Success { (auth: Account) =>
      withAccountAdmin(request, auth) { account =>
        request.content.map { futureContent =>
          futureContent flatMap { jvalue =>
            (jvalue \ "type").validated[String] match {
              case Success(planType) => 
                accountManagement.updateAccount(account.copy(plan = new AccountPlan(planType))) map { 
                  case true => HttpResponse[JValue](OK, content = None)
                  case _ => Responses.failure(InternalServerError, "Account update failed, please contact support.")
                }

              case Failure(error) => 
                Future(HttpResponse[JValue](HttpStatus(BadRequest, "Invalid request body."), content = Some(JString("Could not determine new account type from request body."))))
            }
          }
        } getOrElse {
          Future(HttpResponse[JValue](HttpStatus(BadRequest, "Request body missing."), content = Some(JString("You must provide a JSON object containing a \"type\" field."))))
        }
      }
    }
  }

  val metadata = None
}


//sets plan to "free"
class DeleteAccountPlanHandler(val accountManagement: AccountManager[Future])(implicit ctx: ExecutionContext) 
    extends CustomHttpService[Future[JValue], Account => Future[HttpResponse[JValue]]] 
    with AccountAuthorization
    with Logging {

  val service: HttpRequest[Future[JValue]] => Validation[NotServed, Account => Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => {
    Success { (auth: Account) =>
      withAccountAdmin(request, auth) { account =>
        accountManagement.updateAccount(account.copy(plan = AccountPlan.Free)) map {
          case true => HttpResponse[JValue](OK, content = Some(JObject(List(JField("type",account.plan.planType)))))
          case _ => Responses.failure(InternalServerError, "Account update failed, please contact support.")
        }
      }
    }
  }

  val metadata = None
}


class GetAccountDetailsHandler(val accountManagement: AccountManager[Future])(implicit ctx: ExecutionContext) 
    extends CustomHttpService[Future[JValue], Account => Future[HttpResponse[JValue]]] 
    with AccountAuthorization
    with Logging {

  val service: HttpRequest[Future[JValue]] => Validation[NotServed, Account => Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => {
    Success { (auth: Account) =>
      withAccountAdmin(request, auth) { account =>
        Future(HttpResponse[JValue](OK, content = Some(account.serialize)))
      }
    }
  }

  val metadata = None
}


class DeleteAccountHandler(val accountManagement: AccountManager[Future])(implicit ctx: ExecutionContext) 
    extends CustomHttpService[Future[JValue],  Account => Future[HttpResponse[JValue]]] 
    with AccountAuthorization
    with Logging {

  val service: HttpRequest[Future[JValue]] => Validation[NotServed, Account => Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => {
    Success { (auth: Account) =>
      withAccountAdmin(request, auth) { account =>
        accountManagement.deleteAccount(account.accountId).map { 
          case Some(_) => HttpResponse[JValue](HttpStatus(NoContent))
          case None    => HttpResponse[JValue](HttpStatus(InternalServerError), content = Some(JString("Account deletion failed, please contact support.")))
        }
      }
    }
  }

  val metadata = None
}


// type AccountServiceHandlers // for ctags
