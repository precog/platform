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

class AccountRequiredService[A, B](accountManager: BasicAccountManager[Future], val delegate: HttpService[A, (APIKeyRecord, Path, Account) => Future[B]])
  (implicit err: (HttpFailure, String) => B, dispatcher: MessageDispatcher) 
  extends DelegatingService[A, (APIKeyRecord, Path) => Future[B], A, (APIKeyRecord, Path, Account) => Future[B]] with Logging {
  val service = (request: HttpRequest[A]) => {
    delegate.service(request) map { f => (apiKey: APIKeyRecord, path: Path) =>
      logger.debug("Locating account for request")
      request.parameters.get('ownerAccountId).map { accountId =>
        accountManager.findAccountById(accountId).flatMap {
          case Some(account) => f(apiKey, path, account)
          case None => Future(err(BadRequest, "Unknown account Id: "+accountId))
        }
      }.getOrElse {
        accountManager.listAccountIds(apiKey.apiKey).flatMap { accts =>
          if(accts.size == 1) {
            f(apiKey, path, accts.head)
          } else {
            logger.warn("Unable to determine account ID from api key: " + apiKey.apiKey)
            Future(err(BadRequest, "Unable to identify target account from apiKey"))
          }
        }        
      }
    }
  }

  val metadata =
    Some(AboutMetadata(
      ParameterMetadata('ownerAccountId, None),
      DescriptionMetadata("An explicit or implicit Precog account ID is required for the use of this service.")))
}
