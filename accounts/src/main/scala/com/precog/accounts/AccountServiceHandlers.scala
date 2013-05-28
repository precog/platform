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

import com.precog.common.NetUtils.remoteIpFrom
import com.precog.common.Path
import com.precog.common.accounts._
import com.precog.common.security._
import com.precog.common.security.service.v1
import com.precog.common.security.service.v1.APIKeyDetails
import com.precog.common.services._
import com.precog.util.email.TemplateEmailer

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

import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

import shapeless._

import scalaz._
import scalaz.Validation._
import scalaz.std.option._
import scalaz.syntax.apply._
import scalaz.syntax.plus._
import scalaz.syntax.applicative._
import scalaz.syntax.validation._
import scalaz.syntax.std.option._

object Responses {
  def failure(error: HttpStatusCode, message: String) =
    HttpResponse[JValue](HttpStatus(error), content = Some(JString(message)))

  def failure(error: HttpStatusCode, statMessage: String, message: String) =
    HttpResponse[JValue](HttpStatus(error, statMessage), content = Some(JString(message)))

  def failure(error: HttpStatus, message: String) =
    HttpResponse[JValue](error, content = Some(JString(message)))
}

class AccountServiceHandlers(val accountManager: AccountManager[Future], apiKeyFinder: APIKeyFinder[Future], clock: Clock, rootAccountId: String, rootAPIKey: APIKey, emailer: TemplateEmailer)(implicit executor: ExecutionContext)
    extends Logging {
  import ServiceHandlerUtil._

  def withAccountAdmin[A](accountId: String, auth: Account, request: HttpRequest[_])(f: Account => Future[HttpResponse[JValue]])(implicit executor: ExecutionContext): Future[HttpResponse[JValue]] = {
    implicit val M = new FutureMonad(executor)
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

  object SearchAccountsHandler extends CustomHttpService[Future[JValue], Future[HttpResponse[JValue]]] {
    val service: HttpRequest[Future[JValue]] => Validation[NotServed, Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => {
      Success {
        request.parameters.get('email) match {
          case Some(email) => accountManager.findAccountByEmail(email).map { found =>
            HttpResponse(HttpStatus(OK), content = found.map { account => JArray(JObject("accountId" -> account.accountId.serialize)) }.orElse { Some(JArray()) })
          }

          case None =>
            Promise.successful(Responses.failure(BadRequest, "Missing email address for search in query parameters"))
        }
      }
    }

    val metadata = AndMetadata(
      AboutMetadata(ParameterMetadata('email, None), DescriptionMetadata("The email address associated with the account ID you want to retrieve.")),
      DescriptionMetadata("This endpoint provides capabilities for account search, returning a list of matching account identifiers.")
    )
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

        def followIssuers(currentKey: APIKey, remaining: List[APIKey]): Future[Option[AccountId]] = {
          logger.debug("Finding account for %s with remaining %s".format(currentKey, remaining))
          accountManager.findAccountByAPIKey(currentKey).flatMap {
            case Some(accountId) =>
              logger.debug("Found account for API key: "+keyToFind+" = "+accountId)
              Promise.successful(Some(accountId))

            case None if remaining.nonEmpty =>
              followIssuers(remaining.head, remaining.tail)

            case None =>
              logger.warn("Exhausted parent chain trying to find account for " + keyToFind)
              Promise.successful(None)
            }
        }

        accountManager.findAccountByAPIKey(keyToFind).flatMap {
          case Some(accountId) =>
            logger.debug("Found account for API key: "+keyToFind+" = "+accountId)
            Promise.successful(Some(accountId))

          case None =>
            // No account found, so we need to look up the issuer chain
            apiKeyFinder.findAPIKey(keyToFind, Some(rootAPIKey)).flatMap {
              case Some(APIKeyDetails(_, _, _, _, Nil)) =>
                // We must be looking at the root key
                logger.warn("Empty parent chain trying to find account for " + keyToFind)
                Promise.successful(None)

              case Some(APIKeyDetails(_, _, _, _, issuers)) =>
                logger.debug("No account found for %s, trying issuers %s".format(keyToFind, issuers))
                followIssuers(issuers.head, issuers.tail)

              case None =>
                logger.warn("API key not found trying to find account for %s!".format(keyToFind))
                Promise.successful(None)
            }
        }.map { accountId =>
          HttpResponse[JValue](OK, content = accountId.map(id => WrappedAccountId(id).serialize))
        }
      }
    }

    val metadata = AboutMetadata(
      ParameterMetadata('apiKey, Some("<api key associated with authorizing account>")),
      DescriptionMetadata("Returns the list of accounts associated with the authorized account's API key, or the API key specified by the apiKey request parameter if the authorized account has elevated account management privileges.")
    )
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
            (jv \ "email", jv \ "password", jv \ "profile") match {
              case (JString(email), JString(password), profile) =>
                logger.debug("About to create account for email " + email)
                for {
                  existingAccountOpt <- accountManager.findAccountByEmail(email)
                  accountResponse <-
                    existingAccountOpt map { account =>
                      logger.warn("Creation attempted on existing account %s by %s".format(account.accountId, remoteIpFrom(request)))
                      Promise.successful(Responses.failure(Conflict, "An account already exists with the email address %s. If you feel this is in error, please contact support@precog.com".format(email)))
                    } getOrElse {
                      accountManager.createAccount(email, password, clock.now(), AccountPlan.Free, Some(rootAccountId), profile.minimize) { accountId =>
                        logger.info("Created new account for " + email + " with id " + accountId + " by " + remoteIpFrom(request))
                        apiKeyFinder.createAPIKey(accountId, Some("Root key for account " + accountId), Some("This is your master API key. Keep it secure!")) map {
                          details => details.apiKey
                        }
                      } map { account =>
                        logger.debug("Account successfully created: " + account.accountId)
                        HttpResponse[JValue](OK, content = Some(jobject(jfield("accountId", account.accountId))))
                      }
                    }
                } yield accountResponse

              case _ =>
                val errmsg = "Missing email and/or password fields from request body."
                logger.warn(errmsg + ": " + jv)
                Future(HttpResponse[JValue](HttpStatus(BadRequest, errmsg), content = Some(JString(errmsg))))
            }
          }
        )
      } getOrElse {
        Failure(DispatchError(BadRequest, "Missing request body content."))
      }
    }

    val metadata = DescriptionMetadata("Creates a new account associated with the specified email address, subscribed to the free plan.")
  }

  object CreateAccountGrantHandler extends CustomHttpService[Future[JValue], Future[HttpResponse[JValue]]] {
    val service: HttpRequest[Future[JValue]] => Validation[NotServed, Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => Success { 
        // cannot use withAccountAdmin here because of the ability to add grants to others' accounts.
        request.parameters.get('accountId) map { accountId =>
          accountManager.findAccountById(accountId) flatMap {
            case Some(account) =>
              request.content map { futureContent =>
                futureContent flatMap { jvalue =>
                  jvalue.validated[GrantId]("grantId") match {
                    case Success(grantId) =>
                      apiKeyFinder.addGrant(account.apiKey, grantId) map {
                        case true =>
                          logger.info("Grant added to %s (from %s): %s".format(accountId, remoteIpFrom(request), grantId))
                          HttpResponse(Created)

                        case false =>
                          logger.error("Grant added to %s (from %s) failed for %s".format(accountId, remoteIpFrom(request), grantId))
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

    val metadata = DescriptionMetadata("Adds the grant specified by the grantId property of the request body to the account resource specified in the URL path. The account requesting this change (as determined by HTTP Basic authentication) will be recorded.")
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

    val metadata = DescriptionMetadata("Returns the current plan associated with the account resource specified by the request URL.")
  }

  object GenerateResetTokenHandler extends CustomHttpService[Future[JValue], Future[HttpResponse[JValue]]] {
    val service: HttpRequest[Future[JValue]] => Validation[NotServed, Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => {
      Success {
        request.parameters.get('accountId).flatMap { accountId =>
          request.content.map { futureContent =>
            futureContent.flatMap { jvalue =>
              (jvalue \ "email").validated[String] match {
                case Success(requestEmail) =>
                  accountManager.findAccountById(accountId).flatMap {
                    case Some(account) =>
                      if (account.email == requestEmail) {
                        accountManager.generateResetToken(account).map { resetToken =>
                          try {
                            val params =
                              Map(
                                "token" -> resetToken,
                                "requestor" -> remoteIpFrom(request),
                                "accountId" -> account.accountId,
                                "time" -> (new java.util.Date).toString
                              )

                            emailer.sendEmail(Seq(account.email), "reset.subj.mustache", Seq("reset.eml.txt.mustache" -> "text/plain", "reset.eml.html.mustache" -> "text/html"), params)
                            HttpResponse[JValue](HttpStatus(OK), content = Some(JString("A reset token has been sent to the account email on file")))
                          } catch {
                            case t =>
                              logger.error("Failure sending account password reset email", t)
                              HttpResponse[JValue](HttpStatus(InternalServerError), content = Some(JString("Provided email does not match account for "+ accountId)))
                          }
                        }
                      } else {
                        logger.warn("Password reset request for account %s did not match email on file (%s provided)".format(accountId, requestEmail))
                        Future(HttpResponse[JValue](HttpStatus(Forbidden), content = Some(JString("Provided email does not match account for "+ accountId))))
                      }

                    case None =>
                      logger.warn("Password reset request on non-existent account " + accountId)
                      Future(HttpResponse[JValue](HttpStatus(NotFound), content = Some(JString("Unable to find Account "+ accountId))))
                  }

                case Failure(error) =>
                  logger.warn("Password reset request for account %s without body".format(accountId))
                  Future(HttpResponse[JValue](HttpStatus(BadRequest, "Invalid request body"), content = Some(JString("Missing/invalid email in request body"))))
              }
            }
          }
        }.getOrElse {
          Future(HttpResponse[JValue](HttpStatus(BadRequest, "Missing accountId in request URI."), content = Some(JString("Missing accountId in request URI."))))
        }
      }
    }

    val metadata = DescriptionMetadata("This service is used to generate a password reset token for an account.")
  }

  object PasswordResetHandler extends CustomHttpService[Future[JValue], Future[HttpResponse[JValue]]] {
    val service: HttpRequest[Future[JValue]] => Validation[NotServed, Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => {
      Success {
        ( request.parameters.get('accountId).toSuccess(NonEmptyList("Missing account ID in request URI")) |@|
          request.parameters.get('resetToken).toSuccess(NonEmptyList("Missing reset token in request URI")) |@|
          request.content.toSuccess(NonEmptyList("Missing POST body (new password) in request"))).apply { (accountId, resetToken, futureContent) =>
            futureContent.flatMap { jvalue =>
              jvalue.validated[String]("password") match {
                case Success(newPassword) =>
                  accountManager.resetAccountPassword(accountId, resetToken, newPassword).map {
                    case \/-(true) =>
                      logger.info("Password for account %s successfully reset by %s".format(accountId, remoteIpFrom(request)))
                      HttpResponse[JValue](OK, content = None)

                    case \/-(false) =>
                      logger.warn("Password reset for account %s from %s failed".format(accountId, remoteIpFrom(request)))
                      Responses.failure(InternalServerError, "Account password reset failed, please contact support.")

                    case -\/(error) =>
                      logger.error("Password reset for account %s from %s failed: %s".format(accountId, remoteIpFrom(request), error))
                      Responses.failure(BadRequest, "Password reset attempt failed: " + error)
                  }

                case Failure(error) =>
                  logger.warn("Password reset request for account %s without new password".format(accountId))
                  Future(Responses.failure(BadRequest, "Missing/invalid password in request body"))
              }
            }
        } valueOr { errors =>
          Future(Responses.failure(BadRequest, errors.list.mkString("\n")))
        }
      }
    }

    val metadata = AndMetadata(
      AboutMetadata(ParameterMetadata('resetToken, None), DescriptionMetadata("The account reset token sent to the email address of the account whose password is being reset.")),
      DescriptionMetadata("""The request body must be of the form: {"password": "my new password"}"""),
      DescriptionMetadata("This service can be used to reset your account password.")
    )
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

    val metadata = DescriptionMetadata("Updates the current password for the account resource specified by the request URL.")
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

    val metadata = DescriptionMetadata("Updates the current plan for the account resource specified by the request URL.")
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

    val metadata = DescriptionMetadata("Downgrades the specified account to the free plan.")
  }


  object GetAccountDetailsHandler extends CustomHttpService[Future[JValue], Account => Future[HttpResponse[JValue]]] {
    val service: HttpRequest[Future[JValue]] => Validation[NotServed, Account => Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => {
      logger.debug("Got account details request " + request)
      Success { (auth: Account) =>
        withAccountAdmin(request, auth) { account =>
          Future(HttpResponse[JValue](OK, content = Some(AccountDetails.from(account).jv)))
        }
      }
    }

    val metadata = DescriptionMetadata("Returns the details of the account resource specified by the request URL.")
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

    val metadata = DescriptionMetadata("Disables the account resource specified by the request URL.")
  }
}


// type AccountServiceHandlers // for ctags
