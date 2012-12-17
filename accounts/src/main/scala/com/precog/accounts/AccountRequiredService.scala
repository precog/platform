package com.precog.accounts

import akka.dispatch.{ Future, MessageDispatcher }

import blueeyes.core.service._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._

import com.weiglewilczek.slf4s.Logging

import scalaz._
import scalaz.std.option._
import scalaz.syntax.std.option._

import com.precog.common.Path
import com.precog.common.security._

class AccountRequiredService[A, B](accountFinder: AccountFinder[Future], val delegate: HttpService[A, (APIKeyRecord, Path, AccountId) => Future[B]])
  (implicit err: (HttpFailure, String) => B, dispatcher: MessageDispatcher) 
  extends DelegatingService[A, (APIKeyRecord, Path) => Future[B], A, (APIKeyRecord, Path, AccountId) => Future[B]] with Logging {
  val service = (request: HttpRequest[A]) => {
    delegate.service(request) map { f => (apiKey: APIKeyRecord, path: Path) =>
      logger.debug("Locating account for request with apiKey " + apiKey.apiKey)
      request.parameters.get('ownerAccountId) map { accountId =>
        logger.debug("Using provided ownerAccountId: " + accountId)
        accountFinder.findAccountById(accountId).flatMap {
          case Some(account) => f(apiKey, path, account.accountId)
          case None => Future(err(BadRequest, "Unknown account Id: "+accountId))
        }
      } getOrElse {
        logger.debug("Looking up accounts based on apiKey")
        try {
          accountFinder.listAccountIds(apiKey.apiKey).flatMap { accts =>
            logger.debug("Found accounts: " + accts)
            if(accts.size == 1) {
              f(apiKey, path, accts.head)
            } else {
              logger.warn("Unable to determine account Id from api key: " + apiKey.apiKey)
              Future(err(BadRequest, "Unable to identify target account from apiKey"))
            }
          }
        } catch {
          case e => {
            logger.error("Error locating account from apiKey " + apiKey, e);
            Future(err(BadRequest, "Unable to identify target account from apiKey"))
          }
        } finally {
          logger.debug("Exiting AccountRequiredService handling")
        }
      }
    }
  }

  val metadata =
    Some(AboutMetadata(
      ParameterMetadata('ownerAccountId, None),
      DescriptionMetadata("An explicit or implicit Precog account Id is required for the use of this service.")))
}
