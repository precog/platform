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
import blueeyes.bkka.AkkaTypeClasses._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.json.JsonAST._
import blueeyes.json.xschema.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.xschema.DefaultSerialization.{ DateTimeDecomposer => _, DateTimeExtractor => _, _ }


import blueeyes.bkka.AkkaTypeClasses._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.json.JsonAST._
import blueeyes.json.xschema.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.xschema.DefaultSerialization.{ DateTimeDecomposer => _, DateTimeExtractor => _, _ }
import blueeyes.json.xschema.Extractor._
import blueeyes.core.http.MimeTypes._
import blueeyes.core.data.BijectionsChunkJson._
import blueeyes.core.service.engines.HttpClientXLightWeb
import blueeyes.util.Clock


import HttpHeaders.Authorization


import akka.dispatch.{ ExecutionContext, Future, Await }
import akka.util.Timeout
import akka.util.duration._

import java.security._

import com.weiglewilczek.slf4s.Logging

import scalaz.{ Applicative, Validation, Success, Failure }

import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat


class AuthenticationService[A, B](accountManager: AccountManager[Future], val delegate: HttpService[A, Account => Future[B]])(implicit err: (HttpFailure, String) => B, ctx: ExecutionContext) 
extends DelegatingService[A, Future[B], A, Account => Future[B]] with Logging {
  val service = (request: HttpRequest[A]) => {
    delegate.service(request) map { (f: Account => Future[B]) =>
      request.headers.header[Authorization] flatMap {
        _.basic map {
          case BasicAuthCredentials(email,  password) =>
            accountManager.authAccount(email, password) flatMap { 
              case Some(account)   => f(account)
              case None            => Future(err(Unauthorized, "Credentials provided were formatted correctly, but did not match a known account."))
            }
        }
      } getOrElse {
        Future(err(Unauthorized, "No credentials provided, or Authorization header format was invalid."))
      }
    }
  }
  
  val metadata = Some(AboutMetadata(ParameterMetadata('accountId, None), DescriptionMetadata("A accountId is required for the use of this service.")))
}


class ListAccountsHandler(accountManagement: AccountManagement)(implicit ctx: ExecutionContext) 
extends CustomHttpService[Future[JValue], Account => Future[HttpResponse[JValue]]] with Logging {
  val service: HttpRequest[Future[JValue]] => Validation[NotServed,Account => Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => {
    Success { (auth: Account) =>
      //TODO, if root then can see more
      accountManagement.listAccountIds(auth.accountId).map { 
        case accounts => 
          HttpResponse[JValue](OK, 
                               content = Some(JObject(accounts.map(account => JField("accountId", account.accountId))(collection.breakOut))))
      }
    }
  }

  val metadata = None
}


//returns accountId of account if exists, else creates account, 
//we are working on path accountId.. do we use this to get the account the user wants to create?
//because we also need auth at this stage.. auth will give us the root key for permmissions
class PostAccountHandler(accountManagement: AccountManagement, clock: Clock, securityServiceRoot: String, rootKey: String)(implicit ctx: ExecutionContext) 
extends CustomHttpService[Future[JValue], Future[HttpResponse[JValue]]] with Logging {
   
  val service: HttpRequest[Future[JValue]] => Validation[NotServed, Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => {
    request.content map { futureContent => 
      Success(
        futureContent flatMap { jv =>
          (jv \ "email", jv \ "password") match {
            case (JString(email), JString(password)) =>
              for {
                existingAccountOpt <- accountManagement.findAccountByEmail(email)
                accountResponse <- 
                  existingAccountOpt map { account =>
                    Future(HttpResponse[JValue](OK, content = Some(JObject(List(JField("accountId", account.accountId))))))
                  } getOrElse {
                    def baseGrant(grantType: String, accountId: String) = JObject(
                      JField("type", grantType) ::
                      JField("path", Path(accountId)) ::
                      JField("ownerAccountId", accountId) ::
                      JField("expirationDate", 0) :: Nil
                    )

                    accountManagement.newAccount(email, password, clock.now(), AccountPlan.Free) { (accountId, path) =>
                      val createBody = JObject(
                        JField("grants", JArray(
                          baseGrant("owner", accountId) ::
                          baseGrant("read", accountId) :: 
                          baseGrant("write", accountId) ::
                          baseGrant("reduce", accountId) :: Nil
                        )) :: Nil
                      ) 
                     
                      val client = new HttpClientXLightWeb 
                      client.path(securityServiceRoot).query("apiKey", rootKey)  
                                                      .contentType(application/MimeTypes.json)
                                                      .post[JValue]("apikeys")(createBody) map {
                        case HttpResponse(HttpStatus(OK, _), _, Some(jid), _) => jid.deserialize[String]
                        case HttpResponse(HttpStatus(failure: HttpFailure, reason), _, _, _) => throw new HttpException(failure, reason)
                        case x => throw HttpException(BadGateway, "Unexpected response from the api provisioning service: " + x)
                      }
                    } map { account =>
                      // todo: send email ?
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


class CreateAccountGrantHandler(accountManagement: AccountManagement, securityServiceRoot: String)(implicit ctx: ExecutionContext) 
extends CustomHttpService[Future[JValue], Account =>  Future[HttpResponse[JValue]]] with Logging {
  val service: HttpRequest[Future[JValue]] => Validation[NotServed, Account => Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => {
    Success { (auth: Account) =>
      (for {
        accountId <- request.parameters.get('accountId)
        grantId <- request.parameters.get('grantId)
      } yield {
        accountManagement.findAccountById(accountId) flatMap {
          case Some(account) => 
            val createBody = JObject(List(JField("grantId", JString(grantId)))) 
        
            val client = new HttpClientXLightWeb 
            client.path(securityServiceRoot).query("apiKey", auth.apiKey)
                                            .contentType(application/MimeTypes.json)
                                            .post[JValue]("apikeys/" + account.apiKey + "/grants")(createBody) map {
                                              
              case HttpResponse(HttpStatus(Created, _), _, None, _) => 
                HttpResponse[JValue](OK, content = Some(""))
              
              case _ =>
                HttpResponse[JValue](HttpStatus(InternalServerError), content = Some(JString("could not create grants")))
            }   
                
          case _  => 
            Future(HttpResponse[JValue](HttpStatus(NotFound), content = Some(JString("Unable to find account "+ accountId))))
        }
      }) getOrElse {
        Future(HttpResponse[JValue](HttpStatus(BadRequest, "Missing account or grant id from request parameters."), 
                                    content = Some(JString("Missing account or grant id from request parameters."))))
      }
    }
  }

  val metadata = None
}


//returns plan for account
class GetAccountPlanHandler(accountManagement: AccountManagement)(implicit ctx: ExecutionContext) 
extends CustomHttpService[Future[JValue],Account => Future[HttpResponse[JValue]]] with Logging {
  val service: HttpRequest[Future[JValue]] => Validation[NotServed, Account => Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => {
    Success { (auth: Account) => 
      request.parameters.get('accountId).map { accountId =>
         accountManagement.findAccountById(accountId).map { 
          case Some(account) => 
            HttpResponse[JValue](OK, content = Some(JObject(List(JField("type",account.plan.planType)))))
            
          case _ => HttpResponse[JValue](HttpStatus(NotFound), content = Some(JString("Unable to find Account "+ accountId)))
        }
      } getOrElse {
        Future(HttpResponse[JValue](HttpStatus(BadRequest, "Missing accountId in request URI."), content = Some(JString("Missing accountId in request URI."))))
      }
    }
  }

  val metadata = None
}



//update account Plan
class PutAccountPlanHandler(accountManagement: AccountManagement)(implicit ctx: ExecutionContext) 
extends CustomHttpService[Future[JValue], Account =>Future[HttpResponse[JValue]]] with Logging {
  val service: HttpRequest[Future[JValue]] => Validation[NotServed, Account => Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => {
    Success { (auth: Account) =>
      (for {
        accountId <- request.parameters.get('accountId)
        planType <- request.parameters.get('type)
      } yield {
        accountManagement.findAccountById(accountId) flatMap { 
          case Some(account) => 
            accountManagement.updateAccount(account.copy(plan = new AccountPlan(planType))) map { 
              case true =>
                HttpResponse[JValue](OK, content = Some(JObject(List(JField("type",account.plan.planType)))))

              case _ =>
                HttpResponse[JValue](HttpStatus(InternalServerError, "Failed to update Account"), 
                                     content = Some(JString("Failed to Update Account")))
            }
            
          case _ =>
            Future(HttpResponse[JValue](HttpStatus(NotFound), content = Some(JString("Unable to find Account "+ accountId))))
        }
      }) getOrElse {
        sys.error("fixme")
        Future(HttpResponse[JValue](HttpStatus(BadRequest, "Missing type in request URI."), content = Some(JString("Missing type in request URI."))))
      }
    }
  }

  val metadata = None
}


//sets plan to "free"
class DeleteAccountPlanHandler(accountManagement: AccountManagement)(implicit ctx: ExecutionContext) 
extends CustomHttpService[Future[JValue], Account => Future[HttpResponse[JValue]]] with Logging {
  val service: HttpRequest[Future[JValue]] => Validation[NotServed, Account => Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => {
    Success { (auth: Account) =>
      request.parameters.get('accountId).map { accountId =>
        accountManagement.findAccountById(accountId).map { 
          case Some(account) => 
            accountManagement.updateAccount(account.copy(plan = AccountPlan.Free))
            HttpResponse[JValue](OK, content = Some(JObject(List(JField("type",account.plan.planType)))))
          
          case _ => 
            HttpResponse[JValue](HttpStatus(NotFound), content = Some(JString("Unable to find Account "+ accountId)))
        }
      } getOrElse {
        Future(HttpResponse[JValue](HttpStatus(BadRequest, "Missing accountId in request URI."), 
                                    content = Some(JString("Missing accountId in request URI."))))
      }
    }
  }

  val metadata = None
}


class GetAccountDetailsHandler(accountManagement: AccountManagement)(implicit ctx: ExecutionContext) 
extends CustomHttpService[Future[JValue], Account => Future[HttpResponse[JValue]]] with Logging {
  val service: HttpRequest[Future[JValue]] => Validation[NotServed, Account => Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => {
    Success { (auth: Account) =>
      request.parameters.get('accountId) map { accountId =>
        accountManagement.findAccountById(accountId).map { 
          case Some(account) => HttpResponse[JValue](OK, content = Some(account.serialize))
          case (_)  => HttpResponse[JValue](HttpStatus(NotFound), content = Some(JString("Unable to find Account "+ accountId)))
        }
      } getOrElse {
        Future(HttpResponse[JValue](HttpStatus(BadRequest, "Missing accountId in request URI."), content = Some(JString("Missing accountId in request URI."))))
      }
    }
  }

  val metadata = None
}


class DeleteAccountHandler(accountManagement: AccountManagement)(implicit ctx: ExecutionContext) 
extends CustomHttpService[Future[JValue],  Account => Future[HttpResponse[JValue]]] with Logging {
  val service: HttpRequest[Future[JValue]] => Validation[NotServed, Account => Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => {
    Success { (auth: Account) =>
      request.parameters.get('accountId).map { accountId =>
        accountManagement.deleteAccount(accountId).map { 
          if(_) HttpResponse[JValue](HttpStatus(NoContent))
          else  HttpResponse[JValue](HttpStatus(NotFound), content = Some(JString("Unable to find Account "+accountId)))
        }
      } getOrElse {
        Future(HttpResponse[JValue](HttpStatus(BadRequest, "Missing accountId in request URI."), content = Some(JString("Missing accountId in request URI."))))
      }
    }
  }

  val metadata = None
}


class AccountManagement(val accountManager: AccountManager[Future])(implicit execContext: ExecutionContext) {
  def newTempPassword() : String = {
    accountManager.newTempPassword() 
  }
  
  def newAccount(email: String, password: String, creationDate: DateTime, plan: AccountPlan)(f: (String, Path) => Future[String]): Future[Account] = {
    accountManager.newAccount(email, password, creationDate, plan)(f)
  }
  
  def updateAccount(account: Account) : Future[Boolean] = {
    accountManager.updateAccount(account) 
  }
  
  def findAccountById(accountId: String): Future[Option[Account]] = {
    accountManager.findAccountById(accountId) 
  }
  
  def findAccountByEmail(email: String): Future[Option[Account]] = {
    accountManager.findAccountByEmail(email) 
  }
  
  def accountDetails(accountId: String): Future[Option[Account]] = {
    accountManager.findAccountById(accountId)
  }

  def listAccountIds(apiKey: String)  = {
    if (apiKey == sys.error("what goes here?")) {
      accountManager.listAccountIds("_")
    } else {
      accountManager.listAccountIds(apiKey)
    }
  }
 
  def deleteAccount(accountId: String): Future[Boolean] = {
    accountManager.deleteAccount(accountId).map {
      case Some(_) => true
      case None => false
    }
  } 

  def close() = accountManager.close()
}


// type AccountServiceHandlers // for ctags
