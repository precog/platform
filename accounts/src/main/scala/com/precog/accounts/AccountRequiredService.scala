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

class AccountRequiredService[A, B](accountManager: BasicAccountManager[Future], val delegate: HttpService[A, (APIKeyRecord, Path, AccountId) => Future[B]])
  (implicit err: (HttpFailure, String) => B, dispatcher: MessageDispatcher) 
  extends DelegatingService[A, (APIKeyRecord, Path) => Future[B], A, (APIKeyRecord, Path, AccountId) => Future[B]] with Logging {
  val service = (request: HttpRequest[A]) => {
    delegate.service(request) map { f => (apiKey: APIKeyRecord, path: Path) =>
      logger.debug("Locating account for request with apiKey " + apiKey.apiKey)
      request.parameters.get('ownerAccountId).map { accountId =>
        logger.debug("Using provided ownerAccountId: " + accountId)
        accountManager.findAccountById(accountId).flatMap {
          case Some(account) => f(apiKey, path, account.accountId)
          case None => Future(err(BadRequest, "Unknown account Id: "+accountId))
        }
      }.getOrElse {
        logger.debug("Looking up accounts based on apiKey")
        try {
          accountManager.listAccountIds(apiKey.apiKey).flatMap { accts =>
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
