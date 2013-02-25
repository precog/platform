package com.precog.common
package accounts

import com.precog.common.security._

import akka.dispatch.{ Future, ExecutionContext }

import blueeyes.core.service._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._

import com.weiglewilczek.slf4s.Logging

import scalaz._
import scalaz.std.option._
import scalaz.syntax.std.option._

class AccountRequiredService[A, B](accountFinder: AccountFinder[Future], val delegate: HttpService[A, (APIKey, Path, AccountId) => Future[B]])(implicit err: (HttpFailure, String) => B, executor: ExecutionContext)
extends DelegatingService[A, (APIKey, Path) => Future[B], A, (APIKey, Path, AccountId) => Future[B]] with Logging {
  val service = (request: HttpRequest[A]) => {
    delegate.service(request) map { f => (apiKey: APIKey, path: Path) =>
      logger.debug("Locating account for request with apiKey " + apiKey)

      request.parameters.get('ownerAccountId) map { accountId =>
        logger.debug("Using provided ownerAccountId: " + accountId)
        accountFinder.findAccountDetailsById(accountId) flatMap {
          case Some(account) => f(apiKey, path, account.accountId)
          case None => Future(err(BadRequest, "Unknown account Id: " + accountId))
        }
      } getOrElse {
        logger.trace("Looking up accounts based on apiKey " + apiKey)
        accountFinder.findAccountByAPIKey(apiKey) flatMap {
          case Some(accountId) => f(apiKey, path, accountId)
          case None =>
            logger.warn("Unable to determine account Id from api key: " + apiKey)
            Future(err(BadRequest, "Unable to identify target account from apiKey " + apiKey))
        }
      }
    }
  }

  val metadata =
    Some(AboutMetadata(
      ParameterMetadata('ownerAccountId, None),
      DescriptionMetadata("An explicit or implicit Precog account Id is required for the use of this service.")))
}
