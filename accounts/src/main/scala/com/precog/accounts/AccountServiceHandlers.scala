package com.precog
package accounts

import com.precog.common.NetUtils.remoteIpFrom
import com.precog.common.Path
import com.precog.common.accounts._
import com.precog.common.security._
import com.precog.common.services._

import akka.dispatch.{ ExecutionContext, Future, Promise }
import akka.util.Timeout
import akka.util.duration._

import blueeyes.bkka._
import blueeyes.core.data.ByteChunk
import blueeyes.core.data.DefaultBijections._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.http.MimeTypes._
import blueeyes.core.service._
import blueeyes.core.service.engines.HttpClientXLightWeb
import blueeyes.json._
import blueeyes.json.serialization.{ Extractor, Decomposer, IsoSerialization }
import blueeyes.json.serialization.DefaultSerialization.{ DateTimeDecomposer => _, DateTimeExtractor => _, _ }
import blueeyes.json.serialization.Extractor._

import blueeyes.core.http.MimeTypes._
import blueeyes.core.data._
import DefaultBijections._
import blueeyes.core.service.engines.HttpClientXLightWeb

import blueeyes.util.Clock

import HttpHeaders.Authorization

import com.weiglewilczek.slf4s.Logging

import shapeless._

import scalaz.{ Applicative, Validation, Success, Failure }

import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import com.precog.common.security.service.v1

object Responses {
  def failure(error: HttpStatusCode, message: String) = 
    HttpResponse[JValue](HttpStatus(error), content = Some(JString(message))) 

  def failure(error: HttpStatusCode, statMessage: String, message: String) = 
    HttpResponse[JValue](HttpStatus(error, statMessage), content = Some(JString(message)))
  
  def failure(error: HttpStatus, message: String) = 
    HttpResponse[JValue](error, content = Some(JString(message)))
}

class AccountServiceHandlers(val accountManager: AccountManager[Future], apiKeyFinder: APIKeyFinder[Future], clock: Clock, rootAccountId: String)(implicit executor: ExecutionContext) 
    extends Logging {
  import ServiceHandlerUtil._

  def withAccountAdmin[A](accountId: String, auth: Account, request: HttpRequest[_])(f: Account => Future[HttpResponse[JValue]])(implicit executor: ExecutionContext): Future[HttpResponse[JValue]] = {
    accountManager.findAccountById(accountId) flatMap { 
      case Some(account) =>
        accountManager.hasAncestor(account, auth) flatMap {
          case true  => f(account)
          case false => {
            logger.warn("Unauthorized access attempt to account %s from account %s (%s)".format(accountId, auth.accountId, remoteIpFrom(request)))
            Future(HttpResponse[JValue](HttpStatus(Unauthorized), content = Some(JString("You do not have access to account "+ accountId))))
          }
        }

      case None => 
        Future(HttpResponse[JValue](HttpStatus(NotFound), content = Some(JString("Unable to find Account "+ accountId))))
    }
  }

  def withAccountAdmin[A](request: HttpRequest[_], auth: Account)(f: Account => Future[HttpResponse[JValue]])(implicit executor: ExecutionContext): Future[HttpResponse[JValue]] = {
    request.parameters.get('accountId).map { accountId =>
      withAccountAdmin(accountId, auth, request) { f }
    } getOrElse {
      Future(HttpResponse[JValue](HttpStatus(BadRequest, "Missing accountId in request URI."), content = Some(JString("Missing accountId in request URI."))))
    }
  }

  object ListAccountsHandler extends CustomHttpService[Future[JValue], Account => Future[HttpResponse[JValue]]] {
    val service: HttpRequest[Future[JValue]] => Validation[NotServed,Account => Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => {
      Success { (auth: Account) =>
        val keyToFind = if (auth.accountId == rootAccountId) {
          // Root can send an apiKey query param for the lookup
          request.parameters.get('apiKey).getOrElse(auth.apiKey)
        } else {
          auth.apiKey
        }

        logger.debug("Looking up account ids with account: "+auth.accountId+" for API key: "+keyToFind)
        
        accountManager.findAccountByAPIKey(keyToFind).map { 
          case accountIds =>
            logger.debug("Found accounts for API key: "+keyToFind+" = "+accountIds)
            HttpResponse[JValue](OK, content = Some(accountIds.map(WrappedAccountId(_)).serialize))
        }
      }
    }

    val metadata = None
  }

  //returns accountId of account if exists, else creates account, 
  //we are working on path accountId.. do we use this to get the account the user wants to create?
  //because we also need auth at this stage.. auth will give us the root key for permmissions
  object PostAccountHandler extends CustomHttpService[Future[JValue], Future[HttpResponse[JValue]]] {
    val service: HttpRequest[Future[JValue]] => Validation[NotServed, Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => {
      logger.trace("Got request in PostAccountHandler: " + request)
      request.content map { futureContent => 
        Success(
          futureContent flatMap { jv =>
            (jv \ "email", jv \ "password") match {
              case (JString(email), JString(password)) =>
                logger.debug("About to create account for email " + email)
                for {
                  existingAccountOpt <- accountManager.findAccountByEmail(email)
                  accountResponse <- 
                    existingAccountOpt map { account =>
                      logger.debug("Found existing account: " + account.accountId)
                      Future(HttpResponse[JValue](OK, content = Some(jobject(jfield("accountId", account.accountId)))))
                    } getOrElse {
                      accountManager.newAccount(email, password, clock.now(), AccountPlan.Free, Some(rootAccountId)) { (accountId, path) =>
                        logger.info("Created new account for " + email + " with id " + accountId + " and path " + path + " by " + remoteIpFrom(request))
                        apiKeyFinder.newAPIKey(accountId, path, Some("Root key for account " + accountId), Some("This is your master API key. Keep it secure!")) map {
                          details => details.apiKey
                        }
                      } map { account =>
                        logger.debug("Account successfully created: " + account.accountId)
                        HttpResponse[JValue](OK, content = Some(jobject(jfield("accountId", account.accountId))))
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

  object CreateAccountGrantHandler extends CustomHttpService[Future[JValue], Account =>  Future[HttpResponse[JValue]]] {
    val service: HttpRequest[Future[JValue]] => Validation[NotServed, Account => Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => Success { (auth: Account) =>
        // cannot use withAccountAdmin here because of the ability to add grants to others' accounts.
        request.parameters.get('accountId) map { accountId =>
          accountManager.findAccountById(accountId) flatMap {
            case Some(account) => 
              request.content map { futureContent => 
                futureContent flatMap { jvalue =>
                  jvalue.validated[GrantId]("grantId") match {
                    case Success(grantId) =>
                      apiKeyFinder.addGrant(auth.apiKey, account.apiKey, grantId) map {
                        case true =>
                          logger.info("Grant created by %s (%s): %s".format(auth.accountId, remoteIpFrom(request), grantId))
                          HttpResponse(Created)

                        case false =>
                          logger.error("Grant creation by %s (%s) failed for %s".format(auth.accountId, remoteIpFrom(request), grantId))
                          HttpResponse(InternalServerError, content = Some(JString("Grant creation failed; please contact support.")))
                      }

                    case Failure(error) =>
                      Promise successful badRequest("Could not determine a valid grant ID from request body.")
                  }
                }
              } getOrElse {
                Promise successful badRequest("Missing request body.")
              }
                  
            case _  => 
              Promise.successful(HttpResponse[JValue](HttpStatus(NotFound), content = Some(JString("Unable to find account "+ accountId))))
          }
        } getOrElse {
          Future(badRequest("Missing account Id"))
        }
    }

    val metadata = None
  }


  //returns plan for account
  object GetAccountPlanHandler extends CustomHttpService[Future[JValue],Account => Future[HttpResponse[JValue]]] {
    val service: HttpRequest[Future[JValue]] => Validation[NotServed, Account => Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => {
      Success { (auth: Account) => 
        withAccountAdmin(request, auth) { account =>
          Future(HttpResponse[JValue](OK, content = Some(jobject(jfield("type", account.plan.planType)))))
        }
      }
    }

    val metadata = None
  }


  //update account password
  object PutAccountPasswordHandler extends CustomHttpService[Future[JValue], Account =>Future[HttpResponse[JValue]]] {
    val service: HttpRequest[Future[JValue]] => Validation[NotServed, Account => Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => {
      Success { (auth: Account) =>
        withAccountAdmin(request, auth) { account =>
          request.content map { futureContent =>
            futureContent flatMap { jvalue =>
              (jvalue \ "password").validated[String] match {
                case Success(newPassword) => 
                  accountManager.updateAccountPassword(account, newPassword) map {
                    case true => 
                      logger.info("Password for account %s successfully updated by %s".format(account.accountId, remoteIpFrom(request)))
                      HttpResponse[JValue](OK, content = None)
                    case _ => 
                      logger.error("Password update for account %s from %s failed".format(account.accountId, remoteIpFrom(request)))
                      Responses.failure(InternalServerError, "Account update failed, please contact support.")
                  } 

                case Failure(error) => 
                  logger.warn("Invalid password update body \"%s\" for account %s from %s: %s".format(jvalue.renderCompact, account.accountId, remoteIpFrom(request), error))
                  Future(HttpResponse[JValue](HttpStatus(BadRequest, "Invalid request body."), content = Some(JString("Could not determine replacement password from request body."))))
              }
            }
          } getOrElse {
            logger.warn("Missing password update body for account %s from %s".format(account.accountId, remoteIpFrom(request)))
            Future(HttpResponse[JValue](HttpStatus(BadRequest, "Request body missing."), content = Some(JString("You must provide a JSON object containing a password field."))))
          }
        }
      }
    }

    val metadata = None
  }

  //update account Plan
  object PutAccountPlanHandler extends CustomHttpService[Future[JValue], Account =>Future[HttpResponse[JValue]]] {
    val service: HttpRequest[Future[JValue]] => Validation[NotServed, Account => Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => {
      Success { (auth: Account) =>
        withAccountAdmin(request, auth) { account =>
          request.content.map { futureContent =>
            futureContent flatMap { jvalue =>
              (jvalue \ "type").validated[String] match {
                case Success(planType) => 
                  accountManager.updateAccount(account.copy(plan = new AccountPlan(planType))) map { 
                    case true => 
                      logger.info("Plan changed for %s to %s from %s".format(account.accountId, planType, remoteIpFrom(request)))
                      HttpResponse[JValue](OK, content = None)
                    case _ => 
                      logger.error("Plan change to %s for account %s by %s failed".format(planType, account.accountId, remoteIpFrom(request)))
                      Responses.failure(InternalServerError, "Account update failed, please contact support.")
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
  object DeleteAccountPlanHandler extends CustomHttpService[Future[JValue], Account => Future[HttpResponse[JValue]]] {
    val service: HttpRequest[Future[JValue]] => Validation[NotServed, Account => Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => {
      Success { (auth: Account) =>
        withAccountAdmin(request, auth) { account =>
          accountManager.updateAccount(account.copy(plan = AccountPlan.Free)) map {
            case true => 
              logger.info("Account plan for %s deleted (converted to free plan) by %s".format(account.accountId, remoteIpFrom(request)))
              HttpResponse[JValue](OK, content = Some(jobject(jfield("type",account.plan.planType))))
            case _ => 
              logger.error("Account plan for %s deletion by %s failed".format(account.accountId, remoteIpFrom(request)))
              Responses.failure(InternalServerError, "Account update failed, please contact support.")
          }
        }
      }
    }

    val metadata = None
  }


  object GetAccountDetailsHandler extends CustomHttpService[Future[JValue], Account => Future[HttpResponse[JValue]]] {
    val service: HttpRequest[Future[JValue]] => Validation[NotServed, Account => Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => {
      Success { (auth: Account) =>
        withAccountAdmin(request, auth) { account =>
          import Account.SafeSerialization._
          Future(HttpResponse[JValue](OK, content = Some(account.jv)))
        }
      }
    }

    val metadata = None
  }


  object DeleteAccountHandler extends CustomHttpService[Future[JValue],  Account => Future[HttpResponse[JValue]]] {
    val service: HttpRequest[Future[JValue]] => Validation[NotServed, Account => Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => {
      Success { (auth: Account) =>
        withAccountAdmin(request, auth) { account =>
          accountManager.deleteAccount(account.accountId).map { 
            case Some(_) => 
              logger.warn("Account %s deleted by %s".format(account.accountId, remoteIpFrom(request)))
              HttpResponse[JValue](HttpStatus(NoContent))
            case None    => 
              logger.error("Account %s deletion by %s failed".format(account.accountId, remoteIpFrom(request)))
              HttpResponse[JValue](HttpStatus(InternalServerError), content = Some(JString("Account deletion failed, please contact support.")))
          }
        }
      }
    }

    val metadata = None
  }
}


// type AccountServiceHandlers // for ctags
