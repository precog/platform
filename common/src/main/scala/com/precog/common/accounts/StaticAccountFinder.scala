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
package com.precog.common
package accounts

import com.weiglewilczek.slf4s.Logging

import org.joda.time.DateTime

import org.streum.configrity.Configuration

import scalaz.Monad
import scalaz.syntax.monad._

import com.precog.common._
import com.precog.common.security._

class StaticAccountFinder[M[+_]: Monad](accountId: AccountId, apiKey: APIKey, rootPath: Option[String] = None, email: String = "static@precog.com") extends AccountFinder[M] with Logging {
  private[this] val details = Some(AccountDetails(accountId, email, new DateTime(0), apiKey, Path(rootPath.getOrElse("/" + accountId)), AccountPlan.Root))

  logger.debug("Constructed new static account manager. All queries resolve to \"%s\"".format(details.get))

  def findAccountByAPIKey(apiKey: APIKey) = Some(accountId).point[M]

  def findAccountDetailsById(accountId: AccountId) = details.point[M]
}
